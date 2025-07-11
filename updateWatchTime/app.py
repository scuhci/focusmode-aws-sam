import json
from focus_utils import user_table, CORS_HEADERS, check_id, update_last_active_time, update_user_stage, decimal_to_int

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
    id: str = requested_body.get("prolificId")
    stage_to_update: str = str(requested_body.get("stage"))
    watch_time: int = requested_body.get("watchTime") 

    is_missing = check_id(id)
    if is_missing: 
        return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "message": "User is not yet onboarded."
                }),
        }

    # update the last active timestamp for user
    update_last_active_time(id)

    # update the stgae info if it in time stamp.
    Stage_Status_Response = update_user_stage(id)
    parsed_body = json.loads(Stage_Status_Response["body"]) 

    # Now you can safely access "data"
    data = parsed_body["data"]
    message = parsed_body["message"]


    user_table.update_item(
        Key={"User_Id": id},
            UpdateExpression="SET StageWatchTimes.#stageKey = :watch_time",
            ExpressionAttributeNames={
                "#stageKey": stage_to_update
            },
            ExpressionAttributeValues={
                ":watch_time": watch_time
            },
            ReturnValues="ALL_NEW"
    )

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "stage_status": data, 
            "stage_status_message": message,
            "message": "Watch time updated successfully."
        }, default=decimal_to_int),
    }