import json
import os
import requests
from datetime import datetime
import boto3
import unicodedata
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")
SSM_PARAM = os.environ.get("SSM_PARAM")


def handler(event, context):
    """Get books from the nytimes API, store raw data and new books to S3"""
    ssm_client = boto3.client("ssm", region_name="us-east-1")
    s3_client = boto3.client("s3")

    ssm_response = ssm_client.get_parameter(Name=SSM_PARAM, WithDecryption=True)
    api_key = ssm_response["Parameter"]["Value"]
    logging.info("Getting book categories available...")
    book_categories = get_book_categories(api_key)
    uploaded_keys = []
    for category in book_categories:
        try:
            process_books_from_category(category, api_key, uploaded_keys, s3_client)
        except:
            logging.warning(
                f"There was a problem getting {category} books skipping to the next one..."
            )
    return uploaded_keys


def process_books_from_category(category, api_key, uploaded_keys, s3):
    """Process books from the given category"""
    logging.info(f"Getting {category} books...")
    r = requests.get(
        f"https://api.nytimes.com/svc/books/v3/lists.json?api-key={api_key}&list={category}"
    ).json()
    logging.info(f"Storing raw {category} books data to S3")
    store_raw_data(r, s3, category)
    logging.info(f"Extracting {category} books details...")
    books = extract_books(r)
    logging.info("Checking number of new books...")
    new_books = [book for book in books if not exists_in_dynamo(book)]
    if len(new_books) == 0:
        logging.info(f"There are no {category} new books!")
        return
    logging.info(f"There are {len(new_books)} new {category} books!")
    for book in new_books:
        logging.info(f"Uploading {book['title']}...")
        key = upload_to_s3(book, s3)
        uploaded_keys.append(key)


def get_book_categories(api_key):
    """Get the list of book categories available in the API"""
    r = requests.get(
        f"https://api.nytimes.com/svc/books/v3/lists/names.json?api-key={api_key}"
    ).json()
    results = r["results"]
    return [l["list_name_encoded"] for l in results]


def store_raw_data(response, s3, category):
    """Store the raw response from an API in S3"""
    today = datetime.today().strftime("%Y-%m-%d")
    try:
        s3.put_object(
            Body=json.dumps(response),
            Bucket=S3_BUCKET,
            Key=f"raw/nytimes-api/{today}/{category}.json",
        )
    except Exception as e:
        logging.exception("There was a problem uploading the raw data to S3!")
        raise e


def extract_books(response):
    """Extract books from the API response"""
    books = []
    for result in response["results"]:
        books.append(result["book_details"][0])
    return books


def exists_in_dynamo(book):
    """Check whether a book exists in DynamoDB or not based on its ISBN13"""
    dynamo_client = boto3.client("dynamodb", region_name="us-east-1")

    isbn13 = book["primary_isbn13"]
    condition = "isbn13 = :isbn13"
    attributes = {":isbn13": {"S": isbn13}}
    try:
        response = dynamo_client.query(
            TableName=DYNAMO_TABLE,
            KeyConditionExpression=condition,
            ExpressionAttributeValues=attributes,
        )
    except Exception as e:
        logging.exception("There was a problem while checking in DynamoDB!")
        raise e
    matching_books = response.get("Items", [])
    if len(matching_books) != 0:
        return True
    return False


def upload_to_s3(book, s3):
    """Upload book details as json in S3.

    # TODO: If a book has more than one author, duplicate the book as many times as authors
    """
    author = normalize_name(book["author"])
    isbn13 = book["primary_isbn13"]
    key = f"clean/{author}/{isbn13}.json"
    try:
        s3.put_object(Body=json.dumps(book), Bucket=S3_BUCKET, Key=key)
    except Exception as e:
        logging.exception("There was a problem uploading the book to S3!")
        raise e
    return key


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
