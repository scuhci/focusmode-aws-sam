import json
import boto3
from focus_utils import CORS_HEADERS, USER_TABLE_NAME, check_query_parameters, check_id, get_datetime_obj, update_last_active_time, update_user_stage


def lambda_handler(event, context):
    """Returns the current stage status for the user. 

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
    missing_parameters_message = check_query_parameters(event["queryStringParameters"], ["id"])
    if missing_parameters_message:
        return missing_parameters_message
    
    # get query parameters and body
    id: str = event["queryStringParameters"]["id"]
    
    # check to see if the Prolific ID is valid
    missing_id_message = check_id(id)
    if missing_id_message:
        return missing_id_message

    # update the last active timestamp for user
    update_last_active_time(id)

    # update the stgae info if it in time stamp.
    response = update_user_stage(id)
    parsed_body = json.loads(response["body"]) 

    # Now you can safely access "data"
    data = parsed_body["data"]
    message = parsed_body["message"]
    # print()
    # print("----------------------------------")
    # print("Stage update response: ")
    # print(data)
    # print("-----------------------------------")
    # print()

    # user_item = user_table.get_item(Key={"User_Id": id})
    # print()
    # print("***************************************")
    # print("Response object: ")
    # print(user_item["Item"])
    # print("***************************************")
    # print()
    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "Stage_Status": data,
            "message": f"Received: {message}",
        }),
    }
    
