import os
import json
import boto3 # type: ignore
import hashlib
import time
import random
import requests # type: ignore
import pandas as pd # type: ignore


from boto3.dynamodb.conditions import Key # type: ignore
from datetime import datetime, timedelta
from decimal import Decimal
from googleapiclient.discovery import build # type: ignore


# constants:
USER_TABLE_NAME = "focusmode-FocusModeUserTable-6JC0TNI2RB93"               #os.environ.get("UserTableName", None)
DATA_TABLE_NAME = "focusmode-FocusModeDataCollectionTable-1KRCB5ZWJ6ONL"    #os.environ.get("DataTableName", None)
ADMIN_TABLE_NAME = "focusmode-FocusModeAdminTable-1L8IZJNFRPT8F"            #os.environ.get("AdminTableName", None)
USER_PREFERENCE_DATA_TABLE_NAME = "focusmode-FocusModeUserPreferenceDataTable-1GDK11Q0RIAAO" #os.environ.get("UserPreferenceDataTableName") 
DAILY_SURVEY_DATA_TABLE_NAME = "focusmode-FocusModeDailySurveyResponseTable-NESGY2X0XTDN" #os.environ.get("DailySurveyDataTableName")
POST_STAGE_SURVEY_DATA_TABLE_NAME = "focusmode-FocusModePostStageSurveyResponseTable-1I0Z60LUCHJ13" #os.environ.get("PostStageSurveyDataTableName")

CORS_HEADERS = {
    "Access-Control-Allow-Headers" : "Content-Type",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET"
}

YOUTUBE_API_KEY = os.environ["YouTubeApiKey"]

dynamodb = boto3.resource("dynamodb")
admin_table = dynamodb.Table(ADMIN_TABLE_NAME)
user_table = dynamodb.Table(USER_TABLE_NAME)
user_pref_data_table = dynamodb.Table(USER_PREFERENCE_DATA_TABLE_NAME)


def update_last_active_time(user_id : str):

    missing_id_message = check_id(user_id)
    if missing_id_message:
        return missing_id_message

    try:
        current_timestamp = get_current_datetime_str()
        response = user_table.update_item(
            Key={"User_Id": user_id},
            UpdateExpression="SET Last_Active_At_Time = :ts",
            ExpressionAttributeValues={":ts": current_timestamp},
            ReturnValues="UPDATED_NEW"
        )
        
        # Return only the updated timestamp
        return {"User_Id": user_id, "Last_Active_At_Time": response["Attributes"]["Last_Active_At_Time"]}
    except Exception as e:
        return {"error": f"Failed to update Last_Active_At_Time: {str(e)}"}


def get_next_stage(user_stage_orders, current_stage):
    for i, stage_number in enumerate(user_stage_orders):
        if stage_number == current_stage and i != len(user_stage_orders)-2:
            return user_stage_orders[i+1]
    return None

def decimal_to_int(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError("Type not serializable")


def getStageResponseObject(response_object, user_id, is_stage_changed, is_study_completed):
    data = {
            "user_Id": user_id,
            "current_stage": response_object["Current_Stage"],
            "is_stage_changed": is_stage_changed,
            "is_study_completed": is_study_completed
        }
    return data

def get_current_study_stage(stage_start_times: dict[str, str], last_active_str: str) -> int:
    last_active = get_datetime_obj(last_active_str)
    
    # Convert to list of tuples: (stage_number, datetime_object)
    stage_entries = [
        (stage, get_datetime_obj(start_time)) for stage, start_time in stage_start_times.items()
    ]

    # Sort by datetime so we can find the latest stage that started before last active
    stage_entries.sort(key=lambda x: x[1])

    current_stage = 0
    for stage, start_dt in stage_entries:
        if last_active >= start_dt:
            current_stage = int(stage)
        else:
            break

    return current_stage


def is_study_over(stage_start_times: dict[str, str], stage_sequence: list[int], last_active_str: str) -> bool:
    last_active = get_datetime_obj(last_active_str)
    
    final_stage = stage_sequence[-1]
    final_start = get_datetime_obj(stage_start_times[str(final_stage)])
    final_end = final_start + timedelta(days=7)

    return last_active >= final_end

def update_user_stage(user_id : str):
    missing_id_message = check_id(user_id)
    if missing_id_message:
        return missing_id_message
    
    try:
        # Fetch user details based on primary key
        response = user_table.get_item(Key={"User_Id": user_id})
        response = response["Item"]
       
        current_study_stage = response.get("Current_Stage")
        user_stage_order_list = response["Stage_Order_List"]
        stage_start_times = response["Stage_Start_Times"]
        last_active_timestamp = response["Last_Active_At_Time"]

        stage = get_current_study_stage(stage_start_times, last_active_timestamp)
        is_study_completed = is_study_over(stage_start_times, user_stage_order_list, last_active_timestamp)

        if not current_study_stage and stage == 0:
            first_stage = user_stage_order_list[0]
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression="SET Current_Stage = :new_stage",
                ExpressionAttributeValues={":new_stage": first_stage},
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            
            data = getStageResponseObject(databaseAttributes, user_id, True, False)
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"First stage for the user started successfully."
                },  default=decimal_to_int),
            }
        
        if is_study_completed:
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression=(
                    f"SET Current_Stage = :new_stage,"
                    f"User_Completed_Stages = list_append(User_Completed_Stages, :last_stage)"
                ),
                ExpressionAttributeValues={
                    ":new_stage": stage,
                    ":last_stage": [current_study_stage]
                },
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            data = getStageResponseObject(databaseAttributes, user_id, True, True)
            return {
                    "statusCode": 200,
                    "headers": CORS_HEADERS,
                    "body": json.dumps({
                        "data": data,
                        "message": f"Study for the user with id: {user_id} completed."
                    }, default=decimal_to_int),
                }

        if stage != current_study_stage:
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression=(
                    f"SET Current_Stage = :new_stage,"
                    f"User_Completed_Stages = list_append(User_Completed_Stages, :last_stage)"
                ),
                ExpressionAttributeValues={
                    ":new_stage": stage,
                    ":last_stage": [current_study_stage]
                },
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            data = getStageResponseObject(databaseAttributes, user_id, True, False)
            return {
                        "statusCode": 200,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "data": data,
                            "message": f"started a new stage for the user as previous is completed",
                        }, default=decimal_to_int),
                    }
        else:
            # No need to change any stage information
            # return response with user_id:
            data = getStageResponseObject(response, user_id, False, False)
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"No stage update as user is still in the current stage time limit"
                }, default=decimal_to_int),
            }

        
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "data": {
                    "error": f"Failed to update user stage information for user {user_id}: {str(e)}",
                },
                "message": "ERROR: Failed to update user stage information"
            }),
        }

def check_query_parameters(event_query_string_parameters: list[str], required_parameters: list[str]):
    # missing all parameters
    if event_query_string_parameters == None:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Missing the query parameter(s): {", ".join(required_parameters)}"
            }),
        }
    
    parameters_missing = set(required_parameters) - set(event_query_string_parameters)
    
    # missing some parameters
    if len(parameters_missing) != 0:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Missing the query parameter(s): {", ".join(parameters_missing)}"
            }),
        }
    
    # no parameters missing!
    return None

def check_id(prolific_id: str) -> bool:
    prolific_ids = admin_table.get_item(Key={"id": "prolific_ids"})

    if prolific_id in prolific_ids['Item']['data']:
        return None
    else:
        return {
            "statusCode": 401,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Unauthorized"
            }),
        }


def fetch_and_insert_user_entry(prolificId, newEntry):
    entry_id = f"{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    # Step 1: Query existing entries for the same prolificId and sessionId
    response = user_pref_data_table.query(
        KeyConditionExpression=Key('prolificId').eq(prolificId),
        FilterExpression='sessionId = :sid',
        ExpressionAttributeValues={
            ':sid': newEntry['sessionId']
        }
    )

    items = response.get('Items', [])

    # Step 2: Sort by timestamp descending
    items.sort(key=lambda x: datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00')), reverse=True)

    # Step 3: Get latest three entries
    latest_three = items[:3]

    # Step 4: Flatten previous focus/category data into current entry
    for i in range(1, 4):
        entry = latest_three[i - 1] if i - 1 < len(latest_three) else {}

        newEntry[f'focusMode_{i}'] = entry.get('focus', None)
        newEntry[f'categoryId_{i}'] = (
            entry.get('youTubeApiData', {})
                 .get('snippet', {})
                 .get('categoryId', None)
        )

    # Step 5: Insert the new entry
    item_to_insert = {
        'prolificId': prolificId,
        'Id': entry_id,
        **newEntry
    }

    result = user_pref_data_table.put_item(Item=item_to_insert)
    return result, newEntry

# Function to retrive the youtube data for given youtube video_id
def fetch_youtube_data(video_id):
    if not video_id:
        return None

    url = (
        "https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet,statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        return data["items"][0] if data.get("items") else None

    except requests.exceptions.RequestException as e:
        print(f"YouTube API request failed: {str(e)}")
        return None

# Utility to generate a unique verification code (e.g., SHA-256 of prolificId + timestamp)
def generate_verification_code(prolific_id: str) -> str:
    timestamp = str(int(time.time() * 1000)) 
    to_hash = f"{prolific_id}-{timestamp}"
    hash_value = hashlib.sha256(to_hash.encode()).hexdigest()
    return hash_value


def generate_weekly_stage_start_times(start_ts_str: str, stage_order_list: list[int]) -> dict[str, str]:
    start_dt = get_datetime_obj(start_ts_str)
    stage_map = {}
    for i, stage in enumerate(stage_order_list):
        stage_start = start_dt + timedelta(days=7 * i)
        stage_map[str(stage)] = format_datetime_str(stage_start)
    return stage_map

def format_datetime_str(datetime: datetime) -> str: 
    return datetime.isoformat(timespec='seconds')

def get_current_datetime_str() -> str:
    return format_datetime_str(datetime.now())

def get_datetime_obj(datetime_str: str) -> datetime:
    return datetime.fromisoformat(datetime_str)






def normalize_category_names(cat):
    return cat.replace('&', 'and').strip()

def get_unique_video_categories():
  """
  Retrieves a unique list of video categories from the YouTube Data API.

  Returns:
    A set of unique video category titles.
  """
  youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
  region_code = "US"
  category_request = youtube.videoCategories().list(part="snippet", regionCode=region_code)
  category_response = category_request.execute()

  unique_categories = set()
  category_id_to_name = {}
  for category in category_response['items']:
    formattedCategory = normalize_category_names(category['snippet']['title'])
    unique_categories.add(formattedCategory)
    category_id_to_name[category['id']] = formattedCategory

  return list(unique_categories), category_id_to_name


# JSON to pandas convertor:
def flatten_dict(d, parent_key="", sep="."):
    """
    Recursively flattens a nested dictionary using dot notation.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def parse_video_entry_to_df(json_obj: dict) -> pd.DataFrame:
    """
    Parses a single video entry JSON object into a flattened DataFrame row.
    Handles nested structure inside 'newPreferenceData' and attaches 'prolificId' at top level.
    """

    if not isinstance(json_obj, dict):
        raise ValueError("Expected a JSON object (Python dict)")

    prolific_id = json_obj.get("prolificId", None)
    preference_data = json_obj.get("newPreferenceData", {})

    # Add prolificId to the preference data before flattening
    preference_data["prolificId"] = prolific_id

    # Flatten the entire structure (youTubeApiData and others)
    flat_data = flatten_dict(preference_data)

    # Convert to DataFrame
    return pd.DataFrame([flat_data])

def get_time_of_day(hour):
    if 5 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 17:
        return 'afternoon'
    elif 17 <= hour < 21:
        return 'evening'
    else:
        return 'night'


def expand_intent_node(df):
    """
    Parses and expands the 'intentNode' JSON string column into multiple flat columns.

    Args:
        df (pd.DataFrame): Input DataFrame with an 'intentNode' column.

    Returns:
        pd.DataFrame: DataFrame with expanded intent fields.
    """

    def parse_json(value):
        if pd.isna(value):
            return {}

        try:
            # Try parsing directly
            return json.loads(value)
        except json.JSONDecodeError:
            try:
                # Try parsing after unescaping (for cases like: "\"{\"key\":\"value\"}\"")
                return json.loads(json.loads(value))
            except Exception:
                return {}

    # Parse intentNode column into dictionaries
    intent_parsed = df['intentNode'].apply(parse_json)

    # Expand into new columns
    intent_df = intent_parsed.apply(pd.Series)

    # Add prefix to avoid column name clashes
    intent_df.columns = [f"intent_{col}" for col in intent_df.columns]

    # Combine with original DataFrame
    df = pd.concat([df.drop(columns=['intentNode']), intent_df], axis=1)

    return df


def extract_features(df: pd.DataFrame, category_id_to_name: dict) -> pd.DataFrame:
    def extract_features(row):
        try:
            # Time features
            timestamp = pd.to_datetime(row.get('timestamp', pd.Timestamp.now()))
            row['watch_hour'] = timestamp.hour
            row['watch_weekday'] = timestamp.weekday()
            row['is_night'] = (timestamp.hour < 6) or (timestamp.hour > 22)
            row['is_weekend'] = timestamp.weekday() in [5, 6]
            row['watch_time_of_day'] = get_time_of_day(timestamp.hour)

            # YouTube features
            published_at = pd.to_datetime(row.get('youTubeApiData.snippet.publishedAt', pd.Timestamp.now()))
            row['published_hour'] = published_at.hour
            row['published_weekday'] = published_at.weekday()

            row['title_length'] = len(str(row.get('youTubeApiData.snippet.localized.title', '')).split())
            row['desc_length'] = len(str(row.get('youTubeApiData.snippet.localized.description', '')).split())

            row['viewCount'] = pd.to_numeric(row.get('youTubeApiData.statistics.viewCount', 0), errors='coerce')
            row['favoriteCount'] = pd.to_numeric(row.get('youTubeApiData.statistics.favoriteCount', 0), errors='coerce')
            row['commentCount'] = pd.to_numeric(row.get('youTubeApiData.statistics.commentCount', 0), errors='coerce')

            row['engagement_rate'] = (row['favoriteCount'] + row['commentCount']) / (row['viewCount'] + 1)
            row['is_popular'] = row['viewCount'] > 500000

            # Video category
            cat_id = str(row.get('youTubeApiData.snippet.categoryId', ''))
            cat_id_1 = str(row.get('categoryId_1', ''))
            cat_id_2 = str(row.get('categoryId_2', ''))
            cat_id_3 = str(row.get('categoryId_3', ''))
            row['video_category'] = category_id_to_name.get(cat_id, "Unknown")
            row['categoryId_1'] = category_id_to_name.get(cat_id_1, "Unknown")
            row['categoryId_2'] = category_id_to_name.get(cat_id_2, "Unknown")
            row['categoryId_3'] = category_id_to_name.get(cat_id_3, "Unknown")

        except Exception as e:
            row['error'] = str(e)
        return pd.Series(row)

    transformed_df = df.apply(extract_features, axis=1)
    return transformed_df


def drop_unwanted_columns(df):
    """
    Drops unwanted columns including specific fields and those starting with 'youTubeApiData.snippet.thumbnails'.

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: Cleaned DataFrame with specified columns dropped.
    """
    # Columns to drop explicitly
    columns_to_drop = [
        'timestamp',
        'youTubeApiData.snippet.publishedAt',
        'youTubeApiData.id',
        'youTubeApiData.kind',
        'youTubeApiData.etag',
        'youTubeApiData.snippet.channelId',
        'youTubeApiData.snippet.localized.title',
        'youTubeApiData.snippet.localized.description',
        'youTubeApiData.statistics.viewCount',
        'youTubeApiData.statistics.favoriteCount',
        'youTubeApiData.statistics.commentCount'
    ]

    # Add all columns that start with 'youTubeApiData.snippet.thumbnails'
    thumbnail_cols = [col for col in df.columns if col.startswith('youTubeApiData.snippet.thumbnails')]

    # Combine and drop
    all_cols_to_drop = columns_to_drop + thumbnail_cols
    return df.drop(columns=[col for col in all_cols_to_drop if col in df.columns])


def rename_columns_to_last_segment(df):
    """
    Renames only the columns that contain a dot ('.') by extracting the last part after the dot.
    Other column names remain unchanged.

    E.g.:
    - 'youTubeApiData.snippet.categoryId' → 'categoryId'
    - 'prolificId' → 'prolificId' (unchanged)

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Renamed DataFrame.
    """
    new_columns = {
        col: col.split('.')[-1] if '.' in col else col
        for col in df.columns
    }
    return df.rename(columns=new_columns)


def preprocess_video_json_entry(json_obj):
    """
    Full preprocessing pipeline to convert a single JSON video entry to a final processed DataFrame row.

    Args:
        json_obj (dict): Raw JSON object representing one video entry.
        category_id_to_name (dict): Mapping of YouTube categoryId to category name.

    Returns:
        pd.DataFrame: Processed single-row DataFrame.
    """
    _, category_id_to_name = get_unique_video_categories()

    # Step 1: Flatten JSON to DataFrame
    df = parse_video_entry_to_df(json_obj)

    # Step 2: Expand nested intentNode column
    df = expand_intent_node(df)

    # Step 3: Extract time/contextual/video-based features
    df = extract_features(df, category_id_to_name)

    # Step 4: Drop columns not needed for training
    df = drop_unwanted_columns(df)

    # Step 5: Rename remaining columns (e.g., youTubeApiData.snippet.categoryId → categoryId)
    df = rename_columns_to_last_segment(df)

    return df.to_dict(orient="records")[0]