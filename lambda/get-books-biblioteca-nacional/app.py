import requests
from bs4 import BeautifulSoup
import re
import logging
import os
import boto3
import hashlib
# import stat

S3_BUCKET = os.environ.get("S3_BUCKET")

def handler(event, context):
    page = requests.get('https://datos.gob.es/es/catalogo/ea0019768-registros-bibliograficos-de-manuscritos-y-archivos-personales-de-la-biblioteca-nacional-de-espana')
    assert page.status_code == 200, "There was an error getting the page"
    soup = BeautifulSoup(page.content, 'lxml')
    url = get_download_url(soup)
    assert url, "Could not find any URL to download an UTF8 csv"
    filename = download_file(url)
    new_hash = get_hash(filename)
    print('NEW: ', new_hash)
    
    if is_new(new_hash):
        write_to_s3(filename, new_hash)
    else:
        logging.info("The file already exists")


def get_download_url(soup):
    """Find the url that contains a zip file with a csv in UTF8 format"""
    pattern = "https://.*UTF8.*\.zip"
    resources = soup.find(id='dataset-resources')
    for a in resources.find_all('a', href=True):
        href = a['href']
        if re.match(pattern, href):
            return href
    return None

def download_file(url):
    """Download file from url"""
    zip_file = requests.get(url) 
    filename = url.split('/')[-1]

    # st = os.stat(filename)
    # os.chmod(filename, st.st_mode | stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

    with open(f"/tmp/{filename}",'wb') as output_file:
        output_file.write(zip_file.content)
    return filename

def is_new(new_hash):
    older_hashes = get_older_hashes("raw/biblioteca-nacional/")
    print(older_hashes)
    print(new_hash not in older_hashes)
    return new_hash not in older_hashes


# def get_older_files(key):
#     # return list of s3 files in raw
#     return []

def get_older_hashes(prefix):
    # older_files = get_older_files(key)
    # return [get_hash(file) for file in older_files]
    # return a list with the previous downloads' hash
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files = response.get("Contents")
    print(files)
    return [file['Key'].split('/')[-1].split('.')[0] for file in files]

def write_to_s3(filename, new_hash):
    print('writting')
    s3_client = boto3.client("s3")
    s3_client.upload_file(
        f"/tmp/{filename}", 
        S3_BUCKET, 
        f"raw/biblioteca-nacional/{new_hash}.zip")


def get_hash(file):
  
    # A arbitrary (but fixed) buffer
    # size (change accordingly)
    # 65536 = 65536 bytes = 64 kilobytes
    BUF_SIZE = 65536
  
    # Initializing the sha256() method
    sha256 = hashlib.sha256()
  
    # Opening the file provided as
    # the first commandline argument
    with open(f"/tmp/{file}", 'rb') as f:
         
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