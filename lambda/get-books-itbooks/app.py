import json
import os
import requests
from datetime import datetime
import unicodedata
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    """Get books from the itbooks API, store raw data and new books to S3"""
    s3_client = boto3.client("s3")
    logging.info("Requesting books...")
    r = requests.get("https://api.itbook.store/1.0/new").json()
    logging.info("Storing raw data in S3...")
    store_raw_data(r, s3_client)
    books = r["books"]
    logging.info("Checking number of new books...")
    new_books = [book for book in books if not exists_in_dynamo(book)]
    if len(new_books) == 0:
        logging.info("There are no new books!")
        return []
    logging.info(f"There are {len(new_books)} new books!")
    all_new_keys = []
    for book in new_books:
        logging.info("Getting book details...")
        book_details = get_book_details(book)
        logging.info(f"Uploading {book['title']}...")
        uploaded_keys = upload_to_s3(book_details, s3_client)
        all_new_keys.extend(uploaded_keys)
    return all_new_keys


def exists_in_dynamo(book):
    """Check whether a book exists in DynamoDB or not based on its ISBN13"""
    dynamo_client = boto3.client("dynamodb", region_name="us-east-1")
    # print(book)
    isbn13 = book["isbn13"]
    # print(isbn13)
    condition = "isbn13 = :isbn13"
    attributes = {":isbn13": {"S": isbn13}}
    try:
        response = dynamo_client.query(
            TableName=DYNAMO_TABLE,
            IndexName="Isbn13Index",
            KeyConditionExpression=condition,
            ExpressionAttributeValues=attributes,
        )
    except Exception as e:
        logging.exception("There was a problem while checking in DynamoDB!")
        raise e
    # print(response)
    matching_books = response.get("Items", [])
    if len(matching_books) != 0:
        return True
    return False


def get_book_details(book):
    """Get book details using its ISBN13 and the itbooks API"""
    return requests.get(f"https://api.itbook.store/1.0/books/{book['isbn13']}").json()


def upload_to_s3(book, s3):
    """Upload book details as json in S3.

    If a book has more than one author, duplicate the book as many times as authors
    """
    uploaded_keys = []
    # s3 = boto3.client("s3")
    authors = book["authors"].split(", ")  # authors are separated by a ","
    for author in authors:
        author = normalize_name(author)
        key = f"clean/{author}/{book['isbn13']}.json"
        try:
            s3.put_object(Body=json.dumps(book), Bucket=S3_BUCKET, Key=key)
        except Exception as e:
            logging.exception("There was a problem uploading the book to S3!")
            raise e
        uploaded_keys.append(key)
    return uploaded_keys


def store_raw_data(response, s3):
    """Store the raw response from an API in S3"""
    today = datetime.today().strftime("%Y-%m-%d")
    try:
        s3.put_object(
            Body=json.dumps(response),
            Bucket=S3_BUCKET,
            Key=f"raw/itbooks-api/{today}.json",
        )
    except Exception as e:
        logging.exception("There was a problem uploading the raw data to S3!")
        raise e


def strip_accents(s):
    """Get rid of accents

    https://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-normalize-in-a-python-unicode-string
    """
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def normalize_name(s):
    """Remove dots, underscores and accents"""
    s = s.replace(".", "").replace(" ", "_").lower()
    s = strip_accents(s)
    return s
