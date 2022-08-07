import json
import os

S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")

def handler(event, context):
    print("alooooooooooo")
    body = {
        "message": f"Getting books from ItBooks API V222222222222222222222 {S3_BUCKET} and DOckeeeeeeerrrrr {DYNAMO_TABLE}",
        "input": event,
    }

    return {"statusCode": 200, "body": json.dumps(body)}