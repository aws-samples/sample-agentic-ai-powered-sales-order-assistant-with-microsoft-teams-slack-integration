import json
import boto3
import requests
import time
from botocore.exceptions import ClientError
from json.decoder import JSONDecodeError
from requests.exceptions import RequestException

def setup_aws_clients():
    """Initialize AWS clients with error handling"""
    try:
        bedrock_client = boto3.client("bedrock-runtime")
        secret_client = boto3.client("secretsmanager", region_name="us-east-1")
        lambda_client = boto3.client("lambda")
        return bedrock_client, secret_client, lambda_client
    except Exception as e:
        raise Exception(f"Failed to initialize AWS clients: {str(e)}")

def get_s4_credentials(secret_client):
    """Retrieve S4 credentials from Secrets Manager"""
    try:
        get_secret_value_response = secret_client.get_secret_value(
            SecretId="S4_System_Details"
        )
        secret = json.loads(get_secret_value_response["SecretString"])
        S4_username = secret.get("S4_username")
        S4_password = secret.get("S4_password")
        
        if not S4_username or not S4_password:
            raise ValueError("Missing S4 credentials in secrets")
        
        return S4_username, S4_password
    except ClientError as e:
        raise Exception(f"Failed to retrieve secret: {str(e)}")
    except JSONDecodeError as e:
        raise Exception(f"Failed to parse secret JSON: {str(e)}")

def generate_message(
    bedrock_runtime,
    model_id,
    messages,
    max_tokens=512,
    top_p=1,
    temp=0.5,
    system="",
):
    """Generate message using Claude 3 LLM"""
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

def sap_odata_url(lambda_client, query):
    """Generate OData URL using Lambda function"""
    try:
        body = {"query": query}
        response = lambda_client.invoke(
            FunctionName="SAP-Odata-URL-Generation",
            InvocationType="RequestResponse",
            Payload=json.dumps(body),
        )
        odata_url = json.loads(response["Payload"].read())
        if not odata_url:
            raise ValueError("Empty OData URL received")
        return odata_url
    except (ClientError, JSONDecodeError) as e:
        raise Exception(f"Error generating OData URL: {str(e)}")

def query_salesdata(query, bedrock_client, lambda_client, S4_username, S4_password):
    """Query sales data from SAP system"""
    try:
        SERVICE_URL = sap_odata_url(lambda_client, query)
        
        response = requests.get(
            SERVICE_URL,
            auth=(S4_username, S4_password),
            headers={
                "Prefer": "odata.maxpagesize=500, odata.track-changes"
            },
            timeout=30  # Adding timeout
        )
        
        response.raise_for_status()  # Raise exception for bad HTTP status codes
        contexts = response.json()
        

        system_template = """
                        Please follow the below instructions carefully before responding to the questions.
                        
                        <instructions>
                        1. You are an AI system working on SAP systems and think step by step before providing any response.Do not provide any general overview if you don't know the answer.
                        2. Use the information provided within the context to provide a concise answer.Don't consider any other facts or outlier outside the context provided.
                        3. If you don't know the answer, just say that you don't know, don't try to make up an answer.
                        4. The response should be specific and use statistics or numbers when applicable. 
                        5. Think twice before responding and cross validate the response. 
                        6. The response should include the explanation and logic of the response.
                        7. The response should always be in human readable format.
                        8. For average and summing up don't sum all together but two numbers as a time. For average the number to divide should match the no of items we added. Please follow the below example.
                        </instructions>
                    
                        Here's the context for you to consider:
                        <context></context>
                        
                        Here's the example for you to consider:
                        <example>
                        To Sum up three numbers a,b,c. We first calculate a+b, then we calculate a+b+c for the final result.
                       """
        
        messages = [{"role": "user", "content": query}]
        system = system_template.replace(
            "<context></context>", f" <context>{contexts}</context>"
        )
        
        response = generate_message(
            bedrock_client,
            model_id="anthropic.claude-3-sonnet-20240229-v1:0",
            messages=messages,
            max_tokens=5000,
            temp=0,
            top_p=0.9,
            system=system,
        )
        return response
    except RequestException as e:
        raise Exception(f"Error querying SAP system: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing sales data: {str(e)}")

def lambda_handler(event, context):
    try:
        # Validate input parameters
        required_fields = ["agent", "actionGroup", "function", "inputText"]
        for field in required_fields:
            if field not in event:
                raise ValueError(f"Missing required field: {field}")

        # Initialize AWS clients
        bedrock_client, secret_client, lambda_client = setup_aws_clients()
        
        # Get S4 credentials
        S4_username, S4_password = get_s4_credentials(secret_client)
        
        # Process query
        query = event["inputText"]
        body = query_salesdata(query, bedrock_client, lambda_client, S4_username, S4_password)
        
        # Prepare response
        response_body = {"TEXT": {"body": json.dumps(body)}}
        function_response = {
            "actionGroup": event["actionGroup"],
            "function": event["function"],
            "functionResponse": {"responseBody": response_body},
        }
        action_response = {"messageVersion": "1.0", "response": function_response}
        
        return {
            'statusCode': 200,
            'body': action_response
        }
    
    except ValueError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': f"Invalid input: {str(e)}"})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Internal server error: {str(e)}"})
        }