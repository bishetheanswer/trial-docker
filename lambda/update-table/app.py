import json


def handler(event, context):
    print("Updating tableeee")
    body = {
        "message": "Inserting book to DYNAMODB",
        "input": event,
    }

    return {"statusCode": 200, "body": json.dumps(body)}
    