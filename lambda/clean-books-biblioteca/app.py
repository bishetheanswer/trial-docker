import pandas as pd
import re
import unicodedata
import os
import json
import boto3
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


AUTHOR_COL = "Autor Personas"
S3_BUCKET = os.environ.get('S3_BUCKET')
DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

def handler(event, context):
    # TODO: check if the event is empty. If there are no new books this will fail
    csv_file = event['csv_key']
    csv_path = download_from_s3(key=csv_file, file_path="/tmp/books.csv")
    df = pd.read_csv(csv_path, sep=';')
    logging.info(f"Before checking in Dynamo {df.shape}")
    # df['exists_in_dynamo'] = df['idBNE'].apply(lambda x: exists_in_dynamo(x))
    # df = df[~df["exists_in_dynamo"]]
    # df = df.drop(columns=['exists_in_dynamo'])
    logging.info(f"After checking in Dynamo {df.shape}")
    df = drop_rows_and_cols(df)
    logging.info(f"After drop_rows_and_cols {df.shape}")
    df = clean_author_names(df)
    logging.info(f"After clean_author_names {df.shape}")
    df = df[
        [
            "idBNE",
            "Autor Personas",
            "Autor Entidades",
            "Título",
            "Editorial",
            "Lugar de publicación",
            "Fecha de publicación",
            "Citas o referencias",
            # "Premios",
            "Tema",
            "Género/Forma",
            "Lugar relacionado",
            "id registros relacionados",
            # "Número Bibliografía Nacional",
            "País de publicación",
            "Lengua de publicación",
            "Lengua original",
            "otras lenguas",
            "Tipo de documento",
            "enlaces"
        ]
    ]
    df = df.fillna('')
    logging.info(f"After filling NaNs {df.shape}")
    uploaded_keys = upload_to_s3(df)
    logging.info(f"After uploading to S3 {len(uploaded_keys)}")
    return uploaded_keys

    

def exists_in_dynamo(idBNE):
    """Check whether a book exists in DynamoDB or not based on its idBNE"""
    dynamo_client = boto3.client("dynamodb", region_name="us-east-1")

    condition = "isbn13 = :idBNE"
    attributes = {":idBNE": {"S": idBNE}}
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



# def upload_to_s3(book):
#     uploaded_keys = []
#     s3 = boto3.client("s3")
#     authors = book["authors"].split(", ")  # puede haber mas de un autor
#     for author in authors:
#         author = normalize_name(author)
#         key = f"clean/{author}/{book['isbn13']}.json"
#         s3.put_object(Body=json.dumps(book), Bucket=S3_BUCKET, Key=key)
#         uploaded_keys.append(key)
#     return uploaded_keys




def drop_rows_and_cols(df):
    """Drop empty rows and columns. Also drop rows without an author"""
    df = df.copy()
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    df = df[df[AUTHOR_COL].notna()]
    df = df[df["Título"].notna()]
    return df


def clean_author_names(df):
    df = df.copy()
    df[AUTHOR_COL] = (  # TODO add examples to each transformation
        df[AUTHOR_COL]
        .apply(lambda x: initial_clean(x))
        .str.split("//")
        .apply(lambda x: remove_empty_list_position(x))
        .explode(AUTHOR_COL)
        .str.split("_")
        .apply(lambda x: swap_name_and_surname(x))
        .apply(lambda x: treat_whitespaces(x))
        .apply(lambda x: "_".join(x))
        .apply(lambda x: strip_accents(x))
        .apply(lambda x: keep_chars_and_separator(x))
        .apply(lambda x: clean_start_and_end(x))
    )
    df = df[
        df[AUTHOR_COL].notna()
    ]  # it is possible for some authors to be NaN after cleaning
    return df


def initial_clean(s):
    s = re.sub("\d", "", s)  # get rid of digits
    s = re.sub("\(.*\)", "", s)  # get rid of parenthesis and what is inside
    s = re.sub(
        ",", "_", s
    )  # need to change comas for other symbol cause they are problematic when splitting an string
    s = s.lower()
    return s


def remove_empty_list_position(l):
    return l[:1]


def swap_name_and_surname(l):
    # the format is [surname, name, title]
    if len(l) == 1:
        return l
    else:
        surname = l[0]
        l[0] = l[1]
        l[1] = surname
        return l


def treat_whitespaces(l):
    for i in range(len(l)):
        l[i] = re.sub("^\ +", "", l[i])
        l[i] = re.sub("\ +$", "", l[i])
        l[i] = re.sub(
            " ", "_", l[i]
        )  # substitute the remaining whitespaces with _: de lucia -> de_lucia
        # l[i] = re.sub('^(?!\w)', '', l[i])
    return l


def strip_accents(s):
    # https://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-normalize-in-a-python-unicode-string
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def keep_chars_and_separator(s):
    return re.sub("[^a-zA-Z_]", "", s)


def clean_start_and_end(s):
    s = re.sub("^_", "", s)
    s =re.sub("_$", "", s)
    return s


def upload_to_s3(df):
    s3 = boto3.client("s3")
    uploaded_keys = []
    for _, row in df.iterrows():
        book = row.to_dict()
        key = f"clean/{book[AUTHOR_COL]}/{book['idBNE']}.json"
        s3.put_object(Body=json.dumps(book), Bucket=S3_BUCKET, Key=key)
        uploaded_keys.append(key)
    return uploaded_keys


def download_from_s3(key, file_path):
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, key, file_path) # no lo borro porque lo sobreescribo
    return file_path