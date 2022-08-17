import json
import os
from faker import Faker
import requests
from datetime import datetime
import boto3


S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")
SSM_PARAM = os.environ.get("SSM_PARAM")


def handler(event, context):
    print("NYTIMES")
    body = {
        "message": f"Getting books from The New York Times API {S3_BUCKET} and {DYNAMO_TABLE}",
        "input": event,
    }
    fake = Faker()
    ssm_client = boto3.client("ssm", region_name="us-east-1")
    # get all the list's names
    # https://api.nytimes.com/svc/books/v3/lists/names.json

    # for each list name get the latest list
    response = ssm_client.get_parameter(
        Name=SSM_PARAM, WithDecryption=True
    )  # TODO env var name
    api_key = response["Parameter"]["Value"]

    list_name = "mass-market-paperback"
    r = requests.get(
        f"https://api.nytimes.com/svc/books/v3/lists.json?api-key={api_key}&list={list_name}"
    ).json()

    # store the raw file in s3
    today = datetime.today().strftime("%Y-%m-%d")
    s3 = boto3.client("s3")
    s3.put_object(
        Body=json.dumps(r),
        Bucket=S3_BUCKET,
        Key=f"raw/nytimes-api/{today}/{list_name}.json",
    )

    all_new_keys = []
    books = extract_books(r)
    new_books = [book for book in books if not exists_in_mongo(book)]
    for book in new_books:
        key = upload_to_s3(book)
        all_new_keys.append(key)

    return all_new_keys


def extract_books(response):
    books = []
    for result in response["results"]:
        books.append(result["book_details"][0])
    return books


def exists_in_mongo(book):
    return False  # TODO


def upload_to_s3(book):
    s3 = boto3.client("s3")
    author = book["author"]
    isbn13 = book["primary_isbn13"]
    key = f"clean/{author}/{isbn13}.json"
    s3.put_object(Body=json.dumps(book), Bucket=S3_BUCKET, Key=key)
    return key
