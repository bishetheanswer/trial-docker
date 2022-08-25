import json
import boto3
import os
import numpy as np
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    source, books = get_info_from_event(event)
    if source == "itbooks-api":
        logging.info(f"Inserting books from {source}")
        insert_itbooks(books)
    elif source == "nytimes-api":
        logging.info(f"Inserting books from {source}")
        insert_nytimes(books)
    elif source == "biblioteca":
        logging.info(f"Inserting books from {source}")
        insert_biblioteca(books)
    else:
        logging.warning(f"Could not detect the source: {source}")


def get_info_from_event(event):
    source = event["source"]
    books = event["books"]
    return source, books


def insert_itbooks(books):
    for book in books:
        book_details = extract_details(book)
        print("Uploading: ", book_details["title"])
        upload_itbook_to_dynamo(book_details)


def insert_nytimes(books):
    for book in books:
        book_details = extract_details(book)
        print("Uploading: ", book_details["title"])
        upload_nytimes_to_dynamo(book_details)


def insert_biblioteca(books):
    logging.info(len(books))
    dynamo_client = boto3.client("dynamodb")
    n_batches = (len(books)//25)+1 # batch_write_item only allows to put 25 items at a time
    batches = np.array_split(np.array(books), n_batches)
    for batch in batches:
        logging.info("Processing batch...")
        batch_requests = []
        books_details = [extract_details(book) for book in batch]
        for book in books_details:
            put_request = craft_put_request_biblioteca(book)
            batch_requests.append(put_request)
        print(len(batch_requests))
        try:
            dynamo_client.batch_write_item(RequestItems={DYNAMO_TABLE: batch_requests})
        except Exception as e:
            logging.error("There has been a problem writting the batch to Dynamo")
            raise e
        logging.info("Batch processed!")


def craft_put_request_biblioteca(book):
    return {
        "PutRequest": {
            "Item": {
                "isbn13": {"S": book["idBNE"]},
                "author": {"S": book["Autor Personas"]},
                # "author_entities": {"S": book["Autor Entidades"]},
                "title": {"S": book["Título"]},
                "publisher": {"S": book["Editorial"]},
                "pub_location": {"S": book["Lugar de publicación"]},
                "pub_date": {"S": book["Fecha de publicación"]},
                "references": {"S": book["Citas o referencias"]},
                "theme": {"S": book["Tema"]},
                "genre": {"S": book["Género/Forma"]},
                # "related_location": {"S": book["Lugar relacionado"]},
                # "related_registries": {"S": book["id registros relacionados"]},
                "pub_country": {"S": book["País de publicación"]},
                "language": {"S": book["Lengua de publicación"]},
                # "original_language": {"S": book["Lengua original"]},
                # "other_languages": {"S": book["otras lenguas"]},
                "document_type": {"S": book["Tipo de documento"]},
                "urls": {"S": book["enlaces"]},
            }
        }
    }


# put_request = {
#             "PutRequest": {
#                 "Item": {
#                     "Tipo": {"S": "Alumno"},
#                     "Registro": {"N": str(ts_actual+i)},
#                     "Especialidad": {"S": "AWS"}
#                 }
#             }
#         }
#         actions_for_table.append(put_request)


# print('These are books from the BIBLIOTECA')
# batch = []
# for book in books:
#     book_details = extract_details(book)
#     batch.append(book_details)
#     if len(batch) == 20:
#         upload_biblioteca_to_dynamo(book_details)
#         pass

# # TODO upload in batches
#     print('Uploading: ', book_details["title"])
#     upload_biblioteca_to_dynamo(book_details)
#     print('Uploaded!!!: ', book_details["title"])


def extract_details(key):
    s3 = boto3.client("s3")
    try:
        s3.download_file(S3_BUCKET, key, "/tmp/book_details.json")
    except Exception as e:
        logging.exception(e)
        logging.error("There has been an error downloading book details!")
    with open("/tmp/book_details.json", "r") as f:
        book_details = json.loads(f.read())
    book_details["author_name"] = key.split("/")[1]  # clean/{author}/{isbn13}.json
    return book_details


def upload_itbook_to_dynamo(book_details):
    dynamo_client = boto3.client("dynamodb")
    try:
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
                "pages": {"N": int(book_details["pages"])},
                "rating": {"N": float(book_details["rating"])},
            },
        )
    except:
        logging.warning(f"There has been problem inserting {book_details['title']}!")


def upload_nytimes_to_dynamo(book_details):
    dynamo_client = boto3.client("dynamodb")
    try:
        dynamo_client.put_item(
            TableName=DYNAMO_TABLE,
            Item={
                "author": {"S": book_details["author_name"]},
                "title": {"S": book_details["title"]},
                "publisher": {"S": book_details["publisher"]},
                "price": {"N": book_details["price"]},
                "isbn10": {"S": book_details["primary_isbn10"]},
                "isbn13": {"S": book_details["primary_isbn13"]},
            },
        )
    except:
        logging.warning(f"There has been problem inserting {book_details['title']}!")


def upload_biblioteca_to_dynamo(book_details):
    # TODO upload in batch
    pass


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
