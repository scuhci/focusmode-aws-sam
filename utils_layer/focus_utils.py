import os
import boto3

ADMIN_TABLE_NAME = "focusmode-FocusModeAdminTable-1L8IZJNFRPT8F" #os.environ.get("AdminTableName", None)

dynamodb = boto3.resource("dynamodb")
admin_table = dynamodb.Table(ADMIN_TABLE_NAME)

def check_id(prolific_id: str) -> bool:
    prolific_ids = admin_table.get_item(Key={"id": "prolific_ids"})

    return prolific_id in prolific_ids['Item']['data']

