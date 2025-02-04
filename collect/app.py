import json
import yaml
from focus_utils import check_query_parameters, check_id

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

    # check to see if query parameters have been sent
    missing_parameters_message = check_query_parameters(event["queryStringParameters"], ["id", "type"])
    if missing_parameters_message:
        return missing_parameters_message
    
    # get query parameters and body
    id: str = event["queryStringParameters"]["id"]
    data_type: str = event["queryStringParameters"]["type"]
    requested_body: dict = json.loads(event["body"])
    
    # check to see if the Prolific ID is valid
    if not check_id(id):
        return {
            "statusCode": 401,
            "headers": {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET"
            },
            "body": json.dumps({
                "message": f"Unauthorized"
            }),
        }
    
    # check to make sure a body was sent
    if not requested_body:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET"
            },
            "body": json.dumps({
                "message": f"POST body is missing"
            }),
        }
    
    # check to see if the data type and respective values posted are valid
    with open('data_types.yaml') as stream:
        try:
            valid_data_types: dict = yaml.safe_load(stream)['data_types']
        except yaml.YAMLError as exc:
            print(exc)

            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Headers" : "Content-Type",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET"
                },
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
                        "headers": {
                            "Access-Control-Allow-Headers" : "Content-Type",
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Methods": "GET"
                        },
                        "body": json.dumps({
                            "message": f"Missing the value: {val}"
                        }),
                    }
                
                # check null value types and non-null value types
                if ((val_type[-1] == "?" and requested_body[val] and not isinstance(requested_body[val], valid_data_type_values_map[val_type[:-1]]))
                    or 
                    (not isinstance(requested_body[val], valid_data_type_values_map[val_type]))):
                    return {
                        "statusCode": 400,
                        "headers": {
                            "Access-Control-Allow-Headers" : "Content-Type",
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Methods": "GET"
                        },
                        "body": json.dumps({
                            "message": f"Invalid value type for value: {val}. Should be {val_type}"
                        }),
                    }

            # the request body passed all the checks!
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Headers" : "Content-Type",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET"
                },
                "body": json.dumps(
                    "Runs!"
                ),
            }
                    
    # no keys matched
    return {
        "statusCode": 400,
        "headers": {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET"
        },
        "body": json.dumps({
            "message": f"The data type key, {data_type}, is not a valid data type"
        }),
    }
