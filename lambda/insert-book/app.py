import json
import boto3
import os

S3_BUCKET = os.environ.get('S3_BUCKET')
DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')


def handler(event, context):
    # dynamo_client = boto3.client("dynamodb")
    # table = dynamo_client.Table(DYNAMO_TABLE)
    source, books = get_info_from_event(event)
    print(books)
    if source == 'itbooks-api':
        print('IT')
        insert_itbooks(books)
    elif source == 'nytimes-api':
        print('NY')
        insert_nytimes(books)
    print('NONE')
    # print(event)


def get_info_from_event(event):
    source = event['source']
    books = event['books']
    return source, books

def insert_itbooks(books):
    print('These are books from the ITBOOKS API')
    for book in books:
        book_details = extract_details(book)
        print('Uploading: ', book_details["title"])
        upload_itbook_to_dynamo(book_details)
        print('Uploaded!!!: ', book_details["title"])

def insert_nytimes(books):
    print('These are books from the NYTIMES API')


def extract_details(key):
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, key, "/tmp/book_details.json") # no lo borro porque lo sobreescribo
    with open("/tmp/book_details.json", "r") as f:
        book_details = json.loads(f.read())
    book_details["author_name"] = key.split('/')[1] #clean/{author}/{isbn13}.json # a book can have multiple authors I have already treated this in the previous lambda
    return book_details


def upload_itbook_to_dynamo(book_details):
    dynamo_client = boto3.client("dynamodb")
    dynamo_client.put_item(
        TableName=DYNAMO_TABLE,
        Item={
            "author": {"S": book_details["author_name"]},
            "title": {"S": book_details["title"]},
            "publisher": {"S": book_details["publisher"]},
            "language": {"S": book_details["language"]},
            "year": {"N": book_details["year"]},
            "isbn10": {"S": book_details["isbn10"]},
            "isbn13": {"S": book_details["isbn13"]},
        },
    )

#     bucket, filename = get_info_from_event(event)
#     author_name = filename.split('/')[0]

#     s3 = boto3.client("s3")
#     s3.download_file(bucket, filename, "/tmp/book_details.json")

#     with open("/tmp/book_details.json", "r") as f:
#         book_details = json.loads(f.read())

#     # book_details["author_name"] = {"S": author_name}
#     # book_details["title"] = {"S": book_details["title"]}

#     # response = table.put_item(Item=book_details)

#     dynamo_client.put_item(
#         TableName=DYNAMO_TABLE,
#         Item={
#             "author_name": {"S": author_name},
#             "title": {"S": book_details["title"]},
#             "publisher": {"S": book_details["publisher"]},
#             "language": {"S": book_details["language"]},
#             "year": {"N": book_details["year"]},
#             "isbn10": {"S": book_details["isbn10"]},
#             "isbn13": {"S": book_details["isbn13"]},
#         },
#     )
#     # Key={
#     #     'fecha_subida': {'S': fecha_subida},
#     #     'etiqueta': {'S': etiqueta}
#     # },


# def get_info_from_event(event):
#     bucket = event["Records"][0]["s3"]["bucket"]["name"]
#     filename = event["Records"][0]["s3"]["object"]["key"]
#     return bucket, filename
