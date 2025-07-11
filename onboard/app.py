import json
import random
import boto3
from focus_utils import CORS_HEADERS, USER_TABLE_NAME, check_query_parameters, get_current_datetime_str, update_last_active_time, generate_weekly_stage_start_times, generate_verification_code


def lambda_handler(event, context):
    """Onboard a user into the FocusMode study and return the User info

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # check to see if query parameters have been sent
    missing_parameters_message = check_query_parameters(event["queryStringParameters"], ["id", "focusmode_categories"])
    if missing_parameters_message:
        return missing_parameters_message
    
    # get query parameters
    id: str = event["queryStringParameters"]["id"]
    focusmode_categories: list[str] = event["queryStringParameters"]["focusmode_categories"].split(";")
    
    # check to see if the participant has onboarded before
    dynamodb = boto3.resource("dynamodb")
    user_table = dynamodb.Table(USER_TABLE_NAME)

    user_item = user_table.get_item(Key={"User_Id": id})
    
    # if user exists
    if 'Item' in user_item:
        update_last_active_time(id)

        # only update a focus mode categories
        response = user_table.update_item(
                Key={"User_Id": id},
                UpdateExpression="SET FocusMode_Categories = :focusmode_categories",
                ExpressionAttributeValues={
                    ":focusmode_categories": focusmode_categories
                },
                ReturnValues="ALL_NEW"
            )
        response = response["Attributes"]
        data = {
            "user_Id": str(response.get("User_Id")),
            "current_stage": int(response.get("Current_Stage")),
            "verification_code": str(response.get("Verification_Code")),
            "is_stage_changed": False,
            "is_study_completed": False
        }
        return {
            "statusCode": 208,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "data": data, 
                "message": "User has already onboarded."
            }),
        }
    else:
        # item does not exist -> user has not onboarded before
        # randomly generate order of stage
        stage_order = list(range(1, 5)) # 5 - 1 = 4 stages starting at #1
        random.shuffle(stage_order)
        current_timestamp = get_current_datetime_str()
        verification_code = generate_verification_code(id)

        stage_watch_time = {
            "1" : 0,
            "2" : 0,
            "3" : 0, 
            "4" : 0
        }
        # add participant to the user database
        user_table.put_item(
            Item={
                    "User_Id": id,
                    "Stage_Order_List": stage_order,
                    "Verification_Code": verification_code,
                    "Last_Active_At_Time": current_timestamp,
                    "Stage_Start_Times" : generate_weekly_stage_start_times(current_timestamp, stage_order),
                    "User_Completed_Stages": [],
                    "Current_Stage": stage_order[0],
                    "FocusMode_Categories": focusmode_categories,
                    "StageWatchTimes": stage_watch_time
                }
            )
        
        data = {
            "user_Id": id,
            "current_stage": stage_order[0],
            "verification_code": verification_code,
            "is_stage_changed": True,
            "is_study_completed": False,
        }
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "data": data, 
                "message": "User onboarded successfully"
            }),
        }
