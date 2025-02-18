import os
import json
import requests
from focus_utils import CORS_HEADERS, check_query_parameters, check_id


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
    missing_parameters_message = check_query_parameters(event["queryStringParameters"], ["id", "query"])
    if missing_parameters_message:
        return missing_parameters_message
    
    # get query parameters
    id: str = event["queryStringParameters"]["id"]
    query: str = event["queryStringParameters"]["query"]
    
    # check to see if the Prolific ID is valid
    missing_id_message = check_id(id)
    if missing_id_message:
        return missing_id_message

    # get env variables
    OPENAI_KEY = os.environ["OpenAIKey"]
    
    try:
        url = 'https://api.openai.com/v1/chat/completions'
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {OPENAI_KEY}"
        }

        prompt = f"I want you to act as a YouTube query classifier. I will provide a YouTube search query and you will respond with one word, either 'focus' or 'regular'. A focus mode involves informative, specific educationally content and research, whereas a regular mode is not merely focused on gaining a skill and is more aligning with popular forms of entertainment. The search query is: {query}"

        body = {
            'model': 'gpt-4o-mini',
            'messages': [
                {'role': 'user', 'content': prompt}
            ],
            'response_format': {
                'type': 'json_schema',
                'json_schema': {
                    "name": "categorization",
                    "schema": {
                    "type": "object",
                    "properties": {
                        "category": { "type": "string" },
                        "explanation": { "type": "string" }
                    },
                    "required": ["category", "explanation"],
                    "additionalProperties": False
                    },
                    "strict": True
                }
            }
        }

        response = requests.post(url, headers=headers, json=body, timeout=30)

        if response.status_code == 200:
            json_response = response.json()

            result = json.loads(json_response['choices'][0]['message']['content'])

    except requests.RequestException as e:
        # Send some context about this error to Lambda Logs
        print(e)

        raise e

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps(
            result
        ),
    }
