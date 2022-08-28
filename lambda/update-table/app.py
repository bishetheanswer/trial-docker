import os
import boto3


DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    dynamo_client = boto3.client("dynamodb", region_name="us-east-1")
    for record in event.get("Records", []):
        record_dynamo = record.get("dynamodb")
        if record_dynamo:
            keys = record_dynamo["Keys"]
            author = keys["author"]
            CONDICION = "ADD n_books :new_book"
            ATRIBUTOS_CONDICION = {":new_book": {"N": "1"}}
            dynamo_client.update_item(
                TableName=DYNAMO_TABLE,
                Key={
                    "author": author,
                },
                UpdateExpression=CONDICION,
                ExpressionAttributeValues=ATRIBUTOS_CONDICION,
            )
            print(author)

    return {"statusCode": 200}
