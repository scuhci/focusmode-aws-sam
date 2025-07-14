import os
import json
import requests # type: ignore
import random
import time
from focus_utils import CORS_HEADERS, update_last_active_time, update_user_stage, fetch_youtube_data, decimal_to_int, preprocess_video_json_entry, fetch_and_insert_user_entry, update_user_with_focus_status, user_table, build_prompt


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
        #print("RAW BODY:", event.get("body", ""))
        req_body = json.loads(event.get("body", "{}"))
    except:
        return{
            "statauCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Invalid JSON in request body"})
        }
    
    # Check for required fields
    if "prolificId" not in req_body or "newPreferenceData" not in req_body:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Missing 'prolificId' or 'newPreferenceData' in request body"})
        }

    # Extract parameters
    id: str = req_body.get("prolificId")
    new_entry = req_body.get("newPreferenceData")

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

    # Fetch the user details fom user table
    user_data = user_table.get_item(Key={"User_Id": id})
    user_data = user_data["Item"]
    user_focus_categories = user_data["FocusMode_Categories"]

    # Fetch the YouTube video data and append into request
    video_id = new_entry.get("youTubeID")
    if video_id:
        youtube_data = fetch_youtube_data(video_id)
        if youtube_data is None:
            return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Enable to featch the youtube data for youtube video id : {video_id}"}, default=decimal_to_int)
        } 
        new_entry["youTubeApiData"] = youtube_data
    
    entry_id = f"{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    _, new_entry = fetch_and_insert_user_entry(id, new_entry, entry_id)

    # print("JSON to be parsed")
    # print(req_body)
    prompt_data = preprocess_video_json_entry(req_body)
    prompt_data["focus_categories"] = user_focus_categories

    # print("Parsed data")
    # print(prompt_data)
    
    try:
        url = 'https://api.openai.com/v1/chat/completions'
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {OPENAI_KEY}"
        }

        #prompt = f"I want you to act as a YouTube query classifier. I will provide a YouTube search query and you will respond with one word, either 'focus' or 'regular'. A focus mode involves informative, specific educationally content and research, whereas a regular mode is not merely focused on gaining a skill and is more aligning with popular forms of entertainment. The search query is: {query}"
        prompt = build_prompt(prompt_data)
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
                        "explanation": { "type": "string" },
                        "explanation_summary": { "type": "string" }
                    },
                    "required": ["category", "explanation", "explanation_summary"],
                    "additionalProperties": False
                    },
                    "strict": True
                }
            }
        }

        max_retries = 3
        for i in range(max_retries):
            try:
                response = requests.post(url, headers=headers, json=body, timeout=30)

                if response.status_code == 200:
                    json_response = response.json()
                    result = json.loads(json_response['choices'][0]['message']['content'])

                    summary = result.get("explanation_summary", "")

                    # Split the summary into parts
                    if "|" in summary:
                        parts = summary.split("|")
                        if len(parts) == 2:
                            confidence_part = parts[0].strip()
                            evidence_part = parts[1].strip()

                            # Remove the "Key Evidence:" prefix safely
                            if evidence_part.lower().startswith("key evidence:"):
                                evidence_text = evidence_part[len("key evidence:"):].strip()

                                # Truncate to 20 words
                                words = evidence_text.split()
                                if len(words) > 20:
                                    evidence_text = " ".join(words[:20]) + "..."

                                # Rebuild the summary
                                summary = f"{confidence_part} | Key Evidence: {evidence_text}"

                    result["explanation_summary"] = summary

                    # ✅ Extract focus status
                    focus_status = result.get("category")
                    update_user_with_focus_status(entry_id, id, focus_status)
                    break  # ✅ Success, exit loop

                elif response.status_code == 429:
                    # Rate limit
                    wait_time = 2 ** i
                    print(f"🔁 Rate limit hit. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 502:
                    # Bad Gateway
                    wait_time = 2 ** i
                    print(f"502 Bad Gateway. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                else:
                    # Other non-retryable errors
                    print(f"❌ Unexpected status code {response.status_code}: {response.text}")
                    break
            except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout) as e:
                wait_time = 2 ** i
                print(f"⏳ Read timeout. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            except Exception as e:
                if "rate_limit" in str(e).lower():
                    wait_time = 2 ** i
                    print(f"🔁 Rate limit exception. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                print(f"❌ Exception: {str(e)}")
                break
        else:
            # This runs only if no break occurred
            update_user_with_focus_status(entry_id, id, False)
            # For failure
            result = {
                "category": "false",
                "explanation": "Max retries reached. Could not retrieve prediction from the model. Setting it too default false",
                "explanation_summary": "Confidence: 50% | Key Evidence: Max retries reached. Could not retrieve prediction from the model. Setting it too default false"
            }
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "stage_status": data,
                    "stage_status_message": message,
                    
                    "result": result
                }),
            }
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
