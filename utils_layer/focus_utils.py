import os
import json
from datetime import datetime, timedelta
import boto3
from decimal import Decimal

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


def is_end_time_between(last_logged_time_str: str, last_active_timestamp: str) -> bool:
    """
    Checks if the last activity was within 24 hours of the last logged date in Stage_Days_Start_Time.

    :param stage_days_start_time: List of timestamps in ISO format for the current stage.
    :param last_active_timestamp: User's last active timestamp in ISO format.
    :return: True if last active time is within 24 hours of the last stage entry, else False.
    """

    last_logged_time = get_datetime_obj(last_logged_time_str)  # last stage entry
    last_active_time = get_datetime_obj(last_active_timestamp)  # Convert last active timestamp to datetime

    # Calculate the valid window (24-hour range)
    valid_until = last_logged_time + timedelta(hours=24)

    return last_logged_time <= last_active_time < valid_until

def get_next_stage(user_stage_orders, current_stage):
    for i, stage_number in enumerate(user_stage_orders):
        if stage_number == current_stage and i != len(user_stage_orders)-2:
            return user_stage_orders[i+1]
    return None

def decimal_to_int(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError("Type not serializable")


def getStageResponseObject(response_object, user_id):
    data = {
            "User_Id": user_id,
            "Current_Stage_Number": response_object["Stage_Number"],
            "Current_Day_Of_Stage": response_object["Current_Day"],
            "Start_Time_For_Stage_Days": response_object["Stage_Days_Start_Time"]
        }
    return data

def update_user_stage(user_id : str):
    missing_id_message = check_id(user_id)
    if missing_id_message:
        return missing_id_message
    
    try:
        # Fetch user details based on primary key
        response = user_table.get_item(Key={"User_Id": user_id})
        response = response["Item"]
       
        stage_id_list = response["Stage_Id_List"]
        user_stage_order_list = response["Stage_Order_List"]
        current_timestamp = get_current_datetime_str()
        
        if not stage_id_list:
            # Create New Stage object, insert it into stage id list and return to client
            new_stage = {
                        "Stage_Number": user_stage_order_list[0],
                        "Current_Day": 1,
                        "Stage_Days_Start_Time": [current_timestamp]
                    }
            
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression="SET Stage_Id_List = list_append(Stage_Id_List, :new_stage)",
                ExpressionAttributeValues={":new_stage": [new_stage]},
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            stage_obj = databaseAttributes["Stage_Id_List"][-1]
            data = getStageResponseObject(stage_obj, user_id)
            
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"New stage for the user created successfully."
                },  default=decimal_to_int),
            }
        
        latest_stage = stage_id_list[-1]
        stage_day_start_times = latest_stage["Stage_Days_Start_Time"]
        last_day_start_time = stage_day_start_times[-1]
        last_active_timestamp = response["Last_Active_At_Time"]

        if not is_end_time_between(last_day_start_time, last_active_timestamp):
            if latest_stage["Current_Day"] < 7:
                # update the curr_day += 1
                # add new start time of curr_time to start time list
                last_index = len(stage_id_list) - 1
                response = user_table.update_item(
                    Key={"User_Id": user_id},
                    UpdateExpression=(
                        f"SET Stage_Id_List[{last_index}].Stage_Days_Start_Time = list_append(Stage_Id_List[{last_index}].Stage_Days_Start_Time, :new_time),"
                        f"Stage_Id_List[{last_index}].Current_Day = Stage_Id_List[{last_index}].Current_Day + :inc_day"
                    ),
                    ExpressionAttributeValues={
                        ":new_time": [current_timestamp],
                        ":inc_day": 1,
                    },
                    ReturnValues="ALL_NEW"
                )
                
                databaseAttributes = response["Attributes"]
                stage_obj = databaseAttributes["Stage_Id_List"][-1]
                data = getStageResponseObject(stage_obj, user_id)
                return {
                        "statusCode": 200,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "data": data,
                            "message": f"User processed to next day of current stage",
                        }, default=decimal_to_int),
                    }
            else:
                
                current_stage_number = latest_stage["Stage_Number"]
                next_stage_number = get_next_stage(user_stage_order_list, current_stage_number)
                
                if next_stage_number:
                    new_stage = {
                        "Stage_Number": next_stage_number,
                        "Current_Day": 1,
                        "Stage_Days_Start_Time": [current_timestamp]
                    }
                    response = user_table.update_item(
                        Key={"User_Id": user_id},
                        UpdateExpression="SET Stage_Id_List = list_append(Stage_Id_List, :new_stage)",
                        ExpressionAttributeValues={":new_stage": [new_stage]},
                        ReturnValues="ALL_NEW"
                    )
                    databaseAttributes = response["Attributes"]
                    stage_obj = databaseAttributes["Stage_Id_List"][-1]
                    data = getStageResponseObject(stage_obj, user_id)
                    return {
                        "statusCode": 200,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "data": data,
                            "message": f"started a new stage for the user as previous is completed",
                        }, default=decimal_to_int),
                    }
                else:
                    data = getStageResponseObject(latest_stage, user_id)
                    return {
                        "statusCode": 200,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "data": data,
                            "message": f"Study for the user with id: {user_id} completed."
                        }, default=decimal_to_int),
                    }
        else:
            # No need to change any stage information
            # return response with user_id:
            data = getStageResponseObject(latest_stage, user_id)
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"No stage update as user is still in the current stage time limit"
                }, default=decimal_to_int),
            }
            
    
    except Exception as e:
        return {"error": f"Failed to update user stgae information for user {user_id}: {str(e)}"}

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
