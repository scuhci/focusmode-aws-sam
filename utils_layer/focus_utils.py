import os
import json
from datetime import datetime, timedelta, timezone
import boto3
from decimal import Decimal
import hashlib
import time

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
user_table = dynamodb.Table(USER_TABLE_NAME)

def update_last_active_time(user_id : str):

    missing_id_message = check_id(user_id)
    if missing_id_message:
        return missing_id_message

    try:
        current_timestamp = get_current_datetime_str()
        response = user_table.update_item(
            Key={"User_Id": user_id},
            UpdateExpression="SET Last_Active_At_Time = :ts",
            ExpressionAttributeValues={":ts": current_timestamp},
            ReturnValues="UPDATED_NEW"
        )
        
        # Return only the updated timestamp
        return {"User_Id": user_id, "Last_Active_At_Time": response["Attributes"]["Last_Active_At_Time"]}
    except Exception as e:
        return {"error": f"Failed to update Last_Active_At_Time: {str(e)}"}


def get_next_stage(user_stage_orders, current_stage):
    for i, stage_number in enumerate(user_stage_orders):
        if stage_number == current_stage and i != len(user_stage_orders)-2:
            return user_stage_orders[i+1]
    return None

def decimal_to_int(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError("Type not serializable")


def getStageResponseObject(response_object, user_id, is_stage_changed, is_study_completed):
    data = {
            "user_Id": user_id,
            "current_stage": response_object["Current_Stage"],
            "is_stage_changed": is_stage_changed,
            "is_study_completed": is_study_completed
        }
    return data

def get_current_study_stage(stage_start_times: dict[str, str], last_active_str: str) -> int:
    last_active = get_datetime_obj(last_active_str)
    
    # Convert to list of tuples: (stage_number, datetime_object)
    stage_entries = [
        (stage, get_datetime_obj(start_time)) for stage, start_time in stage_start_times.items()
    ]

    # Sort by datetime so we can find the latest stage that started before last active
    stage_entries.sort(key=lambda x: x[1])

    current_stage = 0
    for stage, start_dt in stage_entries:
        if last_active >= start_dt:
            current_stage = int(stage)
        else:
            break

    return current_stage


def is_study_over(stage_start_times: dict[str, str], stage_sequence: list[int], last_active_str: str) -> bool:
    last_active = get_datetime_obj(last_active_str)
    
    final_stage = stage_sequence[-1]
    final_start = get_datetime_obj(stage_start_times[str(final_stage)])
    final_end = final_start + timedelta(days=7)

    return last_active >= final_end

def update_user_stage(user_id : str):
    missing_id_message = check_id(user_id)
    if missing_id_message:
        return missing_id_message
    
    try:
        # Fetch user details based on primary key
        response = user_table.get_item(Key={"User_Id": user_id})
        response = response["Item"]
       
        current_study_stage = response.get("Current_Stage")
        user_stage_order_list = response["Stage_Order_List"]
        stage_start_times = response["Stage_Start_Times"]
        last_active_timestamp = response["Last_Active_At_Time"]

        stage = get_current_study_stage(stage_start_times, last_active_timestamp)
        is_study_completed = is_study_over(stage_start_times, user_stage_order_list, last_active_timestamp)

        if not current_study_stage and stage == 0:
            first_stage = user_stage_order_list[0]
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression="SET Current_Stage = :new_stage",
                ExpressionAttributeValues={":new_stage": first_stage},
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            
            data = getStageResponseObject(databaseAttributes, user_id, True, False)
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"First stage for the user started successfully."
                },  default=decimal_to_int),
            }
        
        if is_study_completed:
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression=(
                    f"SET Current_Stage = :new_stage,"
                    f"User_Completed_Stages = list_append(User_Completed_Stages, :last_stage)"
                ),
                ExpressionAttributeValues={
                    ":new_stage": stage,
                    ":last_stage": [current_study_stage]
                },
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            data = getStageResponseObject(databaseAttributes, user_id, True, True)
            return {
                    "statusCode": 200,
                    "headers": CORS_HEADERS,
                    "body": json.dumps({
                        "data": data,
                        "message": f"Study for the user with id: {user_id} completed."
                    }, default=decimal_to_int),
                }

        if stage != current_study_stage:
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression=(
                    f"SET Current_Stage = :new_stage,"
                    f"User_Completed_Stages = list_append(User_Completed_Stages, :last_stage)"
                ),
                ExpressionAttributeValues={
                    ":new_stage": stage,
                    ":last_stage": [current_study_stage]
                },
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            data = getStageResponseObject(databaseAttributes, user_id, True, False)
            return {
                        "statusCode": 200,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "data": data,
                            "message": f"started a new stage for the user as previous is completed",
                        }, default=decimal_to_int),
                    }
        else:
            # No need to change any stage information
            # return response with user_id:
            data = getStageResponseObject(response, user_id, False, False)
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"No stage update as user is still in the current stage time limit"
                }, default=decimal_to_int),
            }

        
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "data": {
                    "error": f"Failed to update user stage information for user {user_id}: {str(e)}",
                },
                "message": "ERROR: Failed to update user stage information"
            }),
        }

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


# Utility to generate a unique verification code (e.g., SHA-256 of prolificId + timestamp)
def generate_verification_code(prolific_id: str) -> str:
    timestamp = str(int(time.time() * 1000)) 
    to_hash = f"{prolific_id}-{timestamp}"
    hash_value = hashlib.sha256(to_hash.encode()).hexdigest()
    return hash_value


def generate_weekly_stage_start_times(start_ts_str: str, stage_order_list: list[int]) -> dict[str, str]:
    start_dt = get_datetime_obj(start_ts_str)
    stage_map = {}
    for i, stage in enumerate(stage_order_list):
        stage_start = start_dt + timedelta(days=7 * i)
        stage_map[str(stage)] = format_datetime_str(stage_start)
    return stage_map

def format_datetime_str(datetime: datetime) -> str: 
    return datetime.isoformat(timespec='seconds')

def get_current_datetime_str() -> str:
    return format_datetime_str(datetime.now())

def get_datetime_obj(datetime_str: str) -> datetime:
    return datetime.fromisoformat(datetime_str)
