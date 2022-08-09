import json
import os
from faker import Faker

S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    print("NYTIMES")
    body = {
        "message": f"Getting books from The New York Times API {S3_BUCKET} and {DYNAMO_TABLE}",
        "input": event,
    }
    fake = Faker()

    return ['NY' + fake.name(), 'NY' + fake.name()]