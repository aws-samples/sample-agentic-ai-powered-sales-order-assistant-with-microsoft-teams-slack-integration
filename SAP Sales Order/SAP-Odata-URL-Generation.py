import boto3
from requests import request
import json
import time
from botocore.exceptions import ClientError
from json.decoder import JSONDecodeError


def setup_aws_clients():
    try:
        bedrock_client = boto3.client("bedrock-runtime")
        bedrock_agent_client = boto3.client("bedrock-agent-runtime")
        secret_client = boto3.client("secretsmanager", region_name="us-east-1")
        return bedrock_client, bedrock_agent_client, secret_client
    except Exception as e:
        raise Exception(f"Failed to initialize AWS clients: {str(e)}")

def get_s4_hostname(secret_client):
    try:
        get_secret_value_response = secret_client.get_secret_value(
            SecretId="S4_System_Details"
        )
        secret = json.loads(get_secret_value_response["SecretString"])
        S4_hostname = secret.get("S4_host_details")
        if not S4_hostname:
            raise ValueError("S4_host_details not found in secrets")
        return S4_hostname
    except ClientError as e:
        raise Exception(f"Failed to retrieve secret: {str(e)}")
    except JSONDecodeError as e:
        raise Exception(f"Failed to parse secret JSON: {str(e)}")



#### Helper function for LLM response - Claude 3
def generate_message(
    bedrock_runtime,
    model_id,
    messages,
    max_tokens=512,
    top_p=1,
    temp=0.5,
    system="",
):
    try:
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": messages,
            "temperature": temp,
            "top_p": top_p,
            "system": system,
        })
        response = bedrock_runtime.invoke_model(body=body, modelId=model_id)
        response_body = json.loads(response.get("body").read())
        return response_body["content"][0]["text"]
    except (ClientError, JSONDecodeError, KeyError) as e:
        raise Exception(f"Error generating message: {str(e)}")


def retrieve(query: str, kbId: str, numberOfResults: int = 5):
    try:
        return bedrock_agent_client.retrieve(
            retrievalQuery={"text": query},
            knowledgeBaseId=kbId,
            retrievalConfiguration={
                "vectorSearchConfiguration": {"numberOfResults": numberOfResults}
            },
        )
    except ClientError as e:
        raise Exception(f"Error retrieving from vector DB: {str(e)}")


#### Helper function to prepare the context based on search results from vector DB
def get_contexts(retrievalResults):
    try:
        contexts = []
        for retrievedResult in retrievalResults:
            contexts.append(retrievedResult["content"]["text"])
        return " ".join(contexts)
    except (KeyError, TypeError) as e:
        raise Exception(f"Error processing retrieval results: {str(e)}")

#### Helper function for dynamic Odata query generation
def Odata_Query_generation(query) -> str:
    try:
        kb_id = "H9BTQMHAEO"  # Knowledge Base ID for Odata-Schema-knowledgebase

        response = retrieve(query, kb_id, 3)
        retrievalResults = response["retrievalResults"]

        contexts_KB = get_contexts(retrievalResults)

        messages = [{"role": "user", "content": query}]
        system_template = """
                        Generate SAP Odata URL format using the below instructions.
                        1. Use the parameters given below to generate the COMPLETE Odata URL with '/sap/opu/odata/sap/'.
                        2. Use <parameter - SALES ORDER> to generate Odata for Sales Order Query
                        4. Use <parameter - WORKCENTER> to generate Odata for Workcenter Query
                        5. Use the table schema details from the context given below.
                           Think step by step. First read the field description carefully then check if it can answer the questions asked. If yes, then choose the corresponding field name for the Odata URL.
                           Before responding, please recheck again if the field name exists as part of the provided table schema
                           Do not make up any field name of your own, if you don't know it, just say you dont have the answer.
                           Just mention the FULL SAP Odata URL nothing else in the response
                        6. If the user request information for both Sales Order and Logistic, please remember you are supposed to provide information only for the sales order. Logistic information will be provided by logisctic systhem.
                        7. Use the <example> below to generate Odata URL for any questions asking for details about a particular Sales Order
                        8. All Odata response data should have a JSON format
                           
                        <context></context>
                        
                        <parameters -  SALES ORDER>
                        service_name = API_SALES_ORDER_SRV
                        Entity Set = A_SalesOrder
                        Format = JSON
                        </parameters -  SALES ORDER>
                        
                        
                        <example>
                        Questions -  Share Sales Order details with sales order id 48
                        Odata URI - /sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrder('48')
                        
                        Questions -  Share delivery status for sales order 48
                        Odata URI - /sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrder?$filter=SalesOrder eq '4'&$select=OverallTotalDeliveryStatus&$format=json
                        
                        </example>
                    """
        system = system_template.replace(
            "<context></context>", f" <context>{contexts_KB}</context>"
    )

        odata_url = generate_message(
            bedrock_client,
            model_id="anthropic.claude-3-sonnet-20240229-v1:0",
            messages=messages,
            max_tokens=512,
            temp=0.5,
            top_p=0.9,
            system=system,
        )

        URI = str(odata_url)
        final_odata_url = f"{S4_hostname}{URI}"
        return final_odata_url
    except Exception as e:
        raise Exception(f"Error generating Odata query: {str(e)}")

def lambda_handler(event, context):
    try:
        # Initialize AWS clients
        global bedrock_client, bedrock_agent_client, S4_hostname
        bedrock_client, bedrock_agent_client, secret_client = setup_aws_clients()
        
        # Get S4 credentials
        S4_hostname = get_s4_hostname(secret_client)
        
        # Validate input
        if not event.get("query"):
            raise ValueError("Query parameter is missing in the event")
            
        query = event["query"]
        Odata_URL = Odata_Query_generation(query)
        
        return {
            'statusCode': 200,
            'body': Odata_URL
        }
    except ValueError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Internal server error: {str(e)}"})
        }