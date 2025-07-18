import json
import yaml # type: ignore
import boto3 # type: ignore
import time
import random
from focus_utils import CORS_HEADERS, DAILY_SURVEY_DATA_TABLE_NAME, POST_STAGE_SURVEY_DATA_TABLE_NAME, POST_STUDY_SURVEY_DATA_TABLE_NAME, update_last_active_time, update_user_stage, decimal_to_int

def lambda_handler(event, context):
    """Used to collect data for the FocusMode Study

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
    data_type: str = requested_body.get("type")

    # # check to see if the Prolific ID is valid
    # missing_id_message = check_id(id)
    # if missing_id_message:
    #     return missing_id_message

    if not data_type:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "Missing type in request body"})
        }

    # update the last active timestamp for user
    update_last_active_time(id)

    # update the stgae info if it in time stamp.
    Stage_Status_Response = update_user_stage(id)
    parsed_body = json.loads(Stage_Status_Response["body"]) 

    # Now you can safely access "data"
    data = parsed_body["data"]
    message = parsed_body["message"]
    
    # check to see if the data type and respective values posted are valid
    with open('data_types.yaml') as stream:
        try:
            valid_data_types: dict = yaml.safe_load(stream)['data_types']
        except yaml.YAMLError as exc:
            print(exc)

            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "message": f"Yaml loading error",
                    "error": exc
                }),
            }
        
    valid_data_type_values_map = {'string': str, 'int': int, 'float': float, 'bool': bool}
    
    # add null options to valid_data_type_values_map
    temp_dict = {}
    for k, v in valid_data_type_values_map.items():
        temp_dict[k + "?"] = v

    valid_data_type_values_map.update(temp_dict)

    # check if the requested body is valid
    for key, values in valid_data_types.items():
        # check to see if the data_type key is valid
        if key == data_type:
            # check to see if all the data_type's values are valid
            for val, val_type in values.items():
                # check value names
                if val not in requested_body.keys() and val_type[-1] != "?":
                    return {
                        "statusCode": 400,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "message": f"Missing the value: {val}"
                        }),
                    }
                            
                # check null value types
                if val_type[-1] == "?" and val in requested_body.keys() and not isinstance(requested_body[val], valid_data_type_values_map[val_type[:-1]]):
                    return {
                        "statusCode": 400,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "message": f"Invalid value type for value: {val}. Should be {val_type}"
                        }),
                    }

                # check non-null value types
                elif val in requested_body.keys() and not isinstance(requested_body[val], valid_data_type_values_map[val_type]):
                    return {
                        "statusCode": 400,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "message": f"Invalid value type for value: {val}. Should be {val_type}"
                        }),
                    }

            # # check if there is an extra key in the body
            # extra_values = set(requested_body.keys()) - set(valid_data_types[data_type].keys())
            # if len(extra_values) != 0:
            #     return {
            #         "statusCode": 400,
            #         "headers": CORS_HEADERS,
            #         "body": json.dumps({
            #             "message": f"Invalid value(s): {", ".join(extra_values)}"
            #         }),
            #     }

            # the requested body passed all the checks and is valid!
            dynamodb = boto3.resource("dynamodb")
            daily_survey_data_table = dynamodb.Table(DAILY_SURVEY_DATA_TABLE_NAME)
            post_stage_survey_data_table = dynamodb.Table(POST_STAGE_SURVEY_DATA_TABLE_NAME)
            post_study_survey_data_table = dynamodb.Table(POST_STUDY_SURVEY_DATA_TABLE_NAME)
            
            requested_body["Id"] = f"{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
            if data_type == "daily_survey":
                daily_survey_data_table.put_item(
                    Item=requested_body
                )
            elif data_type == "post_stage_survey":
                post_stage_survey_data_table.put_item(
                    Item=requested_body
                )
            elif data_type == "post_study_survey":
                post_study_survey_data_table.put_item(
                    Item=requested_body
                )
                
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "stage_status": data, # Sending stage status into response
                    "stage_status_message": message,
                    "message": "Survey response saved successfully!"
                }, default=decimal_to_int),
            }
                    
    # no keys matched
    return {
        "statusCode": 400,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "message": f"The data type key, {data_type}, is not a valid data type"
        }),
    }
