import requests
from bs4 import BeautifulSoup
import re
import logging
import os
import boto3
import hashlib
import zipfile

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET")


def handler(event, context):
    """Get books from Biblioteca Nacional using web scraping"""
    page = requests.get(
        "https://datos.gob.es/es/catalogo/ea0019768-registros-bibliograficos-de-manuscritos-y-archivos-personales-de-la-biblioteca-nacional-de-espana"
    )
    assert page.status_code == 200, "There was an error getting the page"
    soup = BeautifulSoup(page.content, "lxml")
    url = get_download_url(soup)
    assert url, "Could not find any URL to download an UTF8 csv"
    filename = download_file(url)
    new_hash = get_hash(filename)
    logging.info(f"New hash: {new_hash}")

    if is_new(new_hash):
        write_to_s3(
            file_path=f"/tmp/{filename}", key=f"raw/biblioteca-nacional/{new_hash}.zip"
        )
        with zipfile.ZipFile(f"/tmp/{filename}", "r") as zip_ref:
            logging.info("Extracting CSVs...")
            csvs = zip_ref.namelist()
            zip_ref.extractall("/tmp/")
        new_keys = []
        for csv in csvs:
            try:
                key = write_to_s3(
                    file_path=f"/tmp/{csv}", key=f"unzip/{new_hash}/{csv}"
                )
                new_keys.append(key)
            except:
                logging.warning(f"There was an error writting {csv} to S3!")
        return new_keys
    else:
        logging.info("The file already exists")
        return []


def get_download_url(soup):
    """Find the url that contains a zip file with a csv in UTF8 format"""
    logging.info("Finding zip url...")
    pattern = "https://.*UTF8.*\.zip"
    resources = soup.find(id="dataset-resources")
    for a in resources.find_all("a", href=True):
        href = a["href"]
        if re.match(pattern, href):
            return href
    return None


def download_file(url):
    """Download file from url"""
    logging.info("Downloading file...")
    zip_file = requests.get(url)
    filename = url.split("/")[-1]
    with open(f"/tmp/{filename}", "wb") as output_file:
        output_file.write(zip_file.content)
    return filename


def is_new(new_hash):
    """Check whether the hash is new or not"""
    logging.info(f"Checking whether {new_hash} is new...")
    older_hashes = get_older_hashes("raw/biblioteca-nacional/")
    return new_hash not in older_hashes


def get_older_hashes(prefix):
    """Get old hashes from filenames"""
    logging.info("Getting old hashes...")
    s3_client = boto3.client("s3")
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    except Exception as e:
        logging.exception(f"There was a problem listing the objects in {prefix}!")
        raise e
    files = response.get("Contents")
    if not files:
        return []
    return [file["Key"].split("/")[-1].split(".")[0] for file in files]


def write_to_s3(file_path, key):
    """Write file to S3"""
    logging.info(f"Writting {key} to S3...")
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(file_path, S3_BUCKET, key)
    except Exception as e:
        logging.exception(f"There was a problem uploading {key} to S3!")
        raise e
    return key


def get_hash(file):
    """Get hash from a file"""
    # https://www.geeksforgeeks.org/compare-two-files-using-hashing-in-python/
    # An arbitrary (but fixed) buffer
    # size (change accordingly)
    # 65536 = 65536 bytes = 64 kilobytes
    logging.info("Getting file hash...")
    BUF_SIZE = 65536

    # Initializing the sha256() method
    sha256 = hashlib.sha256()

    # Opening the file provided as
    # the first commandline argument
    with open(f"/tmp/{file}", "rb") as f:

        while True:

            # reading data = BUF_SIZE from
            # the file and saving it in a
            # variable
            data = f.read(BUF_SIZE)

            # True if eof = 1
            if not data:
                break

            # Passing that data to that sh256 hash
            # function (updating the function with
            # that data)
            sha256.update(data)

    # sha256.hexdigest() hashes all the input
    # data passed to the sha256() via sha256.update()
    # Acts as a finalize method, after which
    # all the input data gets hashed hexdigest()
    # hashes the data, and returns the output
    # in hexadecimal format
    return sha256.hexdigest()
