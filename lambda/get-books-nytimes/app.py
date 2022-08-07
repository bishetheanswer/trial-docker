import json
import os

S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    print("alooooooooooo")
    body = {
        "message": f"Getting books from The New York Times API {S3_BUCKET} and {DYNAMO_TABLE}",
        "input": event,
    }

    return {"statusCode": 200, "body": json.dumps(body)}