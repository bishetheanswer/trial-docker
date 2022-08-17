import json
import os
from faker import Faker
import requests
from datetime import datetime
import boto3
import logging

S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    s3 = boto3.client("s3")
    r = requests.get("https://api.itbook.store/1.0/new").json()
    store_raw_data(r, s3)
    books = r["books"]
    new_books = [book for book in books if not exists_in_mongo(book)]
    all_new_keys = []
    for book in new_books:
        book_details = get_book_details(book)  # los libros pueden tener varios autores
        uploaded_keys = upload_to_s3(book_details)
        all_new_keys.extend(uploaded_keys)
    return all_new_keys

def exists_in_mongo(book):
    # dynamo_client = boto3.client('dynamodb', region_name='us-east-1')

    # isbn13 = book['isbn13']
    # condition = 'isbn13 = :isbn13'
    # attributes = {':isbn13': {'S': isbn13}}
    # # TODO: usar un query en vez de un scan, para eso hay que hace que isbn13 sea primary key o como se llame
    # response = dynamo_client.scan(TableName=DYNAMO_TABLE, IndexName='isbn13', FilterExpression=condition, ExpressionAttributeValues=attributes)
    # matching_books = response.get('Items', [])
    # if len(matching_books) != 0:
    #     logging.info(f"Book {book['title']} already existst!")
    #     return True
    return False


def get_book_details(book):
    return requests.get(f"https://api.itbook.store/1.0/books/{book['isbn13']}").json()


def upload_to_s3(book):
    uploaded_keys = []
    s3 = boto3.client("s3")
    authors = book["authors"].split(",")  # puede haber mas de un autor
    for author in authors:
        author = author.replace(" ", "-")
        key = f"clean/{author}/{book['isbn13']}.json"
        s3.put_object(Body=json.dumps(book), Bucket=S3_BUCKET, Key=key)
        uploaded_keys.append(key)
    return uploaded_keys

def store_raw_data(response, s3):
    """Store the raw response from an API in S3

    Args:
        response (dict): API response
        s3 (client): S3 boto3 client
    """
    today = datetime.today().strftime("%Y-%m-%d")
    s3.put_object(
        Body=json.dumps(response), Bucket=S3_BUCKET, Key=f"raw/itbooks-api/{today}.json"
    )
