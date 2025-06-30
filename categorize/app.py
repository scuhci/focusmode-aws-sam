import os
import json
import requests # type: ignore
from focus_utils import CORS_HEADERS, USER_PREFERENCE_DATA_TABLE_NAME, check_query_parameters, check_id, update_last_active_time, update_user_stage, fetch_youtube_data, decimal_to_int, preprocess_video_json_entry, fetch_and_insert_user_entry


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
    # parse the request body to extract the required parameter.
    try:
        print("RAW BODY:", event.get("body", ""))
        req_body = json.loads(event.get("body", "{}"))
    except:
        return{
            "statauCode": 400,
            "body": json.dumps({"error": "Invalid JSON in request body"})
        }
    
    # Check for required fields
    if "prolificId" not in req_body or "newPreferenceData" not in req_body:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing 'prolificId' or 'newPreferenceData' in request body"})
        }

    # Extract parameters
    id: str = req_body.get("prolificId")
    query: str = req_body["query"]
    new_entry = req_body.get("newPreferenceData")
    
    # check to see if the Prolific ID is valid
    missing_id_message = check_id(id)
    if missing_id_message:
        return missing_id_message

    # get env variables
    OPENAI_KEY = os.environ["OpenAIKey"]
    

    # update the last active timestamp for user
    update_last_active_time(id)

    # update the stgae info if it in time stamp.
    Stage_Status_Response = update_user_stage(id)
    parsed_body = json.loads(Stage_Status_Response["body"]) 

    # extract stage info data from respose body
    data = parsed_body["data"]
    message = parsed_body["message"]



    # Fetch the YouTube video data and append into request
    video_id = new_entry.get("youTubeID")
    if video_id:
        youtube_data = fetch_youtube_data(video_id)
        if youtube_data is None:
            return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Enable to featch the youtube data for youtube video id : {video_id}"}, default=decimal_to_int)
        } 
        new_entry["youTubeApiData"] = youtube_data
    
    _, new_entry = fetch_and_insert_user_entry(id, new_entry)

    print("JSON to be parsed")
    print(req_body)
    prompt_data = preprocess_video_json_entry(req_body)

    print("Parsed data")
    print(prompt_data)
    
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
            # TODO : Update current user with predicted focus status
            json_response = response.json()

            result = json.loads(json_response['choices'][0]['message']['content'])

    except requests.RequestException as e:
        # Send some context about this error to Lambda Logs
        print(e)

        raise e

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "stage_status": data,
            "stage_status_message": message,
            "result": result
    }),
    }
