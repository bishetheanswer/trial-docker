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
S3_BUCKET = os.environ.get("S3_BUCKET")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    """Clean books from a CSV file"""
    csv_file = event["csv_key"]
    logging.info(f"CSV file: {csv_file}")
    csv_path = download_from_s3(key=csv_file, file_path="/tmp/books.csv")
    df = pd.read_csv(csv_path, sep=";")
    df = drop_rows_and_cols(df)
    df = clean_author_names(df)
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
            "Tema",
            "Género/Forma",
            "Lugar relacionado",
            "id registros relacionados",
            "País de publicación",
            "Lengua de publicación",
            "Lengua original",
            "otras lenguas",
            "Tipo de documento",
            "enlaces",
        ]
    ]
    df = df.fillna("")
    df = df.drop_duplicates(subset=["Autor Personas", "Título"])
    uploaded_keys = upload_to_s3(df)
    return uploaded_keys


def drop_rows_and_cols(df):
    """Drop empty rows and columns. Also drop rows without an author"""
    logging.info("Dropping empty rows and columns...")
    df = df.copy()
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    df = df[df[AUTHOR_COL].notna()]
    df = df[df["Título"].notna()]
    return df


def clean_author_names(df):
    """Apply an exhaustive cleaning process to the authors"""
    logging.info("Cleaning author names...")
    df = df.copy()
    df[AUTHOR_COL] = (
        df[AUTHOR_COL]
        .apply(lambda x: initial_clean(x))
        .str.split("//")
        .apply(lambda x: remove_empty_list_position(x))
    )
    df = df.explode(AUTHOR_COL)
    df[AUTHOR_COL] = (
        df[AUTHOR_COL]
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
    """Lowercase and remove digits and other symbols"""
    s = re.sub("\d", "", s)  # get rid of digits
    s = re.sub("\(.*\)", "", s)  # get rid of parenthesis and what is inside
    s = re.sub(
        ",", "_", s
    )  # need to change comas for other symbol cause they are problematic when splitting a string
    s = s.lower()
    return s


def remove_empty_list_position(l):
    """Return the last element of a list"""
    return l[:-1]


def swap_name_and_surname(l):
    """Swap the name and surname positions in the list containing surname, name and title"""
    # the format is [surname, name, title]
    if len(l) == 1:
        return l
    else:
        surname = l[0]
        l[0] = l[1]
        l[1] = surname
        return l


def treat_whitespaces(l):
    """Remove whitespaces in the beginning and ending and substitute the one in the middle with an underscore"""
    for i in range(len(l)):
        l[i] = re.sub("^\ +", "", l[i])
        l[i] = re.sub("\ +$", "", l[i])
        l[i] = re.sub(
            " ", "_", l[i]
        )  # substitute the remaining whitespaces with _: de lucia -> de_lucia
    return l


def strip_accents(s):
    """Get rid of accents

    https://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-normalize-in-a-python-unicode-string
    """
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def keep_chars_and_separator(s):
    """Keep letters and underscores"""
    return re.sub("[^a-zA-Z_]", "", s)


def clean_start_and_end(s):
    """Remove underscores in the beginning and ending"""
    s = re.sub("^_", "", s)
    s = re.sub("_$", "", s)
    return s


def upload_to_s3(df):
    """Upload books data to S3"""
    s3 = boto3.client("s3")
    uploaded_keys = []
    for _, row in df.iterrows():
        book = row.to_dict()
        key = f"clean/{book[AUTHOR_COL]}/{book['idBNE']}.json"
        logging.info(f"Uploading {key}...")
        try:
            s3.put_object(Body=json.dumps(book), Bucket=S3_BUCKET, Key=key)
            uploaded_keys.append(key)
        except Exception as e:
            logging.warning(f"There was problem uploading {key}")

    return uploaded_keys


def download_from_s3(key, file_path):
    """Download file from S3"""
    s3 = boto3.client("s3")
    logging.info(f"Downloading {key}...")
    try:
        s3.download_file(S3_BUCKET, key, file_path)
    except Exception as e:
        logging.exception(f"There was a problem downloading {key}")
        raise e
    return file_path
