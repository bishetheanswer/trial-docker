import requests
from bs4 import BeautifulSoup
import re
import logging
import os

S3_BUCKET = os.environ.get("S3_BUCKET")

def handler(event, context):
    page = requests.get('https://datos.gob.es/es/catalogo/ea0019768-registros-bibliograficos-de-manuscritos-y-archivos-personales-de-la-biblioteca-nacional-de-espana')
    assert page.status_code == 200, "There was an error getting the page"
    soup = BeautifulSoup(page.content, 'lxml')
    print(type(soup))
    url = get_download_url(soup)
    assert url, "Could not find any URL to download an UTF8 csv"
    filename = download_file(url)
    if is_new(filename):
        write_to_s3(filename)
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
    with open(filename,'wb') as output_file:
        output_file.write(zip_file.content)
    return filename

def is_new(new_file):
    older_hashes = get_older_hashes('s3/path/to/raw/biblioteca/nacional')
    new_hash = get_hash(new_file)
    return new_hash in older_hashes


def get_older_files(key):
    # return list of s3 files in raw
    return []

def get_older_hashes(key):
    older_files = get_older_files(key)
    return [get_hash(file) for file in older_files]

def get_hash(key):
    # Download file
    # Calculate hash
    # Delete file from local storage
    pass
