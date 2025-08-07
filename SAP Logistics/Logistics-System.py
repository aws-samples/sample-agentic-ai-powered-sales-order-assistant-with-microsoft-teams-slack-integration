import boto3
from boto3.dynamodb.conditions import Key
import json
import logging
from botocore.exceptions import ClientError
from typing import Dict, Any, List

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class LogisticsQueryError(Exception):
    """Custom exception for logistics query errors"""
    pass

def setup_dynamodb_table(table_name: str):
    """Initialize DynamoDB table with error handling"""
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        # Verify table exists by making a simple call
        table.table_status
        return table
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"Failed to initialize DynamoDB table: {error_code} - {error_message}")
        raise LogisticsQueryError(f"Database initialization error: {error_message}")
    except Exception as e:
        logger.error(f"Unexpected error initializing DynamoDB: {str(e)}")
        raise LogisticsQueryError("Failed to initialize database connection")

def query_logistic_db(sales_id: str) -> Dict[str, Any]:
    """
    Query logistics information for a specific order ID
    """
    try:
        # Input validation
        if not sales_id or not isinstance(sales_id, str):
            raise ValueError("Invalid sales ID provided")

        table_name = "logistics"
        orders_table = setup_dynamodb_table(table_name)
        
        logger.info(f"Querying logistics data for order ID: {sales_id}")
        response = orders_table.query(
            KeyConditionExpression=Key("order_id").eq(sales_id)
        )
        
        # Validate response
        if "Items" not in response:
            raise LogisticsQueryError("Invalid response format from database")
        
        # Log if no items found
        if not response["Items"]:
            logger.warning(f"No logistics data found for order ID: {sales_id}")
            
        return response

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB query error: {error_code} - {error_message}")
        raise LogisticsQueryError(f"Database query error: {error_message}")
    except ValueError as e:
        logger.error(f"Input validation error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in query_logistic_db: {str(e)}")
        raise LogisticsQueryError(f"Failed to query logistics data: {str(e)}")

def validate_event(event: Dict[str, Any]) -> None:
    """Validate the incoming event"""
    required_fields = ["apiPath", "actionGroup", "httpMethod"]
    for field in required_fields:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

def create_error_response(error_message: str, status_code: int = 400) -> Dict[str, Any]:
    """Create standardized error response"""
    response_body = {"application/json": {"body": json.dumps({"error": error_message})}}
    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": "unknown",
            "apiPath": "unknown",
            "httpMethod": "unknown",
            "httpStatusCode": status_code,
            "responseBody": response_body,
        }
    }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler with error handling"""
    try:
        # Validate incoming event
        validate_event(event)
        
        logger.info(f"Processing request for API path: {event['apiPath']}")
        
        if event["apiPath"] == "/salesorder/{SalesOrderID}":
            # Extract and validate parameters
            parameters = event.get("parameters", [])
            sales_id = None
            
            for parameter in parameters:
                if parameter.get("name") == "SalesOrderID":
                    sales_id = parameter.get("value")
                    break
            
            if not sales_id:
                raise ValueError("SalesOrderID parameter is missing or invalid")
            
            # Query logistics database
            body = query_logistic_db(sales_id)
        else:
            logger.warning(f"Invalid API path requested: {event['apiPath']}")
            raise ValueError(f"Invalid API path: {event['apiPath']}")

        # Prepare successful response
        response_body = {"application/json": {"body": json.dumps(body)}}
        action_response = {
            "actionGroup": event["actionGroup"],
            "apiPath": event["apiPath"],
            "httpMethod": event["httpMethod"],
            "httpStatusCode": 200,
            "responseBody": response_body,
        }

        return {
            "messageVersion": "1.0",
            "response": action_response
        }

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return create_error_response(str(e), 400)
    except LogisticsQueryError as e:
        logger.error(f"Logistics query error: {str(e)}")
        return create_error_response(str(e), 500)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return create_error_response("Internal server error", 500)