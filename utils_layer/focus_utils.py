import os
import json
from datetime import datetime
import boto3

# constants:
USER_TABLE_NAME = "focusmode-FocusModeUserTable-6JC0TNI2RB93"               #os.environ.get("UserTableName", None)
DATA_TABLE_NAME = "focusmode-FocusModeDataCollectionTable-1KRCB5ZWJ6ONL"    #os.environ.get("DataTableName", None)
ADMIN_TABLE_NAME = "focusmode-FocusModeAdminTable-1L8IZJNFRPT8F"            #os.environ.get("AdminTableName", None)

CORS_HEADERS = {
    "Access-Control-Allow-Headers" : "Content-Type",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET"
}

dynamodb = boto3.resource("dynamodb")
admin_table = dynamodb.Table(ADMIN_TABLE_NAME)

def check_query_parameters(event_query_string_parameters: list[str], required_parameters: list[str]):
    # missing all parameters
    if event_query_string_parameters == None:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Missing the query parameter(s): {", ".join(required_parameters)}"
            }),
        }
    
    parameters_missing = set(required_parameters) - set(event_query_string_parameters)
    
    # missing some parameters
    if len(parameters_missing) != 0:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Missing the query parameter(s): {", ".join(parameters_missing)}"
            }),
        }
    
    # no parameters missing!
    return None

def check_id(prolific_id: str) -> bool:
    prolific_ids = admin_table.get_item(Key={"id": "prolific_ids"})

    if prolific_id in prolific_ids['Item']['data']:
        return None
    else:
        return {
            "statusCode": 401,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Unauthorized"
            }),
        }

def format_datetime_str(datetime: datetime) -> str: 
    return datetime.isoformat(timespec='seconds')

def get_current_datetime_str() -> str:
    return format_datetime_str(datetime.now())

def get_datetime_obj(datetime_str: str) -> datetime:
    return datetime.fromisoformat(datetime_str)
