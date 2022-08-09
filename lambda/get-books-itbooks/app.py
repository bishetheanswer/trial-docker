import json
import os
from faker import Faker


S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")

def handler(event, context):
    print("ITBOOKS")
    body = {
        "message": f"Getting books from ItBooks API V222222222222222222222 {S3_BUCKET} and DOckeeeeeeerrrrr {DYNAMO_TABLE}",
        "input": event,
    }
    fake = Faker()

    return ['IT' + fake.name(), 'IT' + fake.name()]