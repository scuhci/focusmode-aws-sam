import json
import random
import boto3
from focus_utils import CORS_HEADERS, USER_TABLE_NAME, check_query_parameters, check_id, get_current_datetime_str


def lambda_handler(event, context):
    """Returns the categorization of a YouTube search query into 'focus' or 'regular' with an explanation 

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
    missing_parameters_message = check_query_parameters(event["queryStringParameters"], ["id", "regular_categories", "focusmode_categories"])
    if missing_parameters_message:
        return missing_parameters_message
    
    # get query parameters
    id: str = event["queryStringParameters"]["id"]
    regular_categories: list[str] = event["queryStringParameters"]["regular_categories"].split(";")
    focusmode_categories: list[str] = event["queryStringParameters"]["focusmode_categories"].split(";")
    
    # check to see if the Prolific ID is valid
    missing_id_message = check_id(id)
    if missing_id_message:
        return missing_id_message
    
    # check to see if the participant has onboarded before
    dynamodb = boto3.resource("dynamodb")
    user_table = dynamodb.Table(USER_TABLE_NAME)

    user_item = user_table.get_item(Key={"User_Id": id})
    
    # if user exists
    if 'Item' in user_item:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "User has already onboarded"
            }),
        }
    else:
        # item does not exist -> user has not onboarded before
        # randomly generate order of stage
        stage_order = list(range(1, 6))
        random.shuffle(stage_order)

        # add participant to the user database
        user_table.put_item(
            Item={
                    "User_Id": id,
                    "Stage_Order_List": stage_order,
                    # start at first stage
                    "Stage_Id_List": {
                        "Stage_Number": stage_order[0],
                        "Current_Day": 1,
                        "Stage_Days_Start_Time": [get_current_datetime_str()]
                    },
                    "Regular_Categories": regular_categories,
                    "FocusMode_Categories": focusmode_categories
                }
            )
        
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps(
                "Received"
            ),
        }
