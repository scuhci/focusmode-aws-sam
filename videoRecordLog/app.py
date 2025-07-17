import json
import time
import random
from focus_utils import video_record_log_table, CORS_HEADERS, check_id, update_last_active_time, update_user_stage, decimal_to_int

def lambda_handler(event, context):
    try:
        requested_body: dict = json.loads(event["body"])
    except (ValueError, TypeError) as e:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Invalid POST body. Must be JSON"
            }),
        }
    
    # check to make sure a body was sent
    if not requested_body:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"POST body is missing"
            }),
        }

    # get query parameters and body
    id: str = str(requested_body.get("prolificId"))

    is_missing = check_id(id)
    if is_missing: 
        return is_missing

    # update the last active timestamp for user
    update_last_active_time(id)

    # update the stgae info if it in time stamp.
    Stage_Status_Response = update_user_stage(id)
    parsed_body = json.loads(Stage_Status_Response["body"]) 

    # Now you can safely access "data"
    data = parsed_body["data"]
    message = parsed_body["message"]

    entry_id = f"{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    item_to_insert = {
        'Id': entry_id,
        **requested_body
    }

    result = video_record_log_table.put_item(Item=item_to_insert)

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "stage_status": data, 
            "stage_status_message": message,
            "message": "Video record logged successfully"
        }, default=decimal_to_int),
    }