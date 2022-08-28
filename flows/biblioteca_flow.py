import prefect
from prefect.storage import GitHub
from prefect import task, Flow, unmapped
import boto3
import botocore
import datetime
import json
from prefect.executors import LocalDaskExecutor
import numpy as np


@task(name="GetBooksBiblioteca") #, cache_for=datetime.timedelta(days=1))
def get_books_biblioteca():
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName="authors-homogenization-dev-get-books-biblioteca-nacional",
        InvocationType="RequestResponse",
    )
    assert response["StatusCode"] < 300
    payload = json.loads(response["Payload"].read())
    return payload[:2]  # TODO return only first because of testing purposes


@task(name="CleanBooksBiblioteca") # , cache_for=datetime.timedelta(days=1))
def clean_books_biblioteca(csv_key):
    config = botocore.config.Config(  # https://github.com/boto/boto3/issues/2424
        read_timeout=900, connect_timeout=900, retries={"max_attempts": 0}
    )
    lambda_client = boto3.client("lambda", config=config)
    response = lambda_client.invoke(
        FunctionName="authors-homogenization-dev-clean-books-biblioteca",
        InvocationType="RequestResponse",
        Payload=json.dumps({"csv_key": csv_key}),
    )
    assert response["StatusCode"] < 300
    payload = json.loads(response["Payload"].read())
    return payload


@task(name="CreateBooksBatches")
def create_books_batches(keys):
    books = np.array(keys)
    batches = np.array_split(books, 10)
    return [list(l) for l in batches]


@task(name="InsertBooks")
def insert_books(books, source):
    config = botocore.config.Config(  # https://github.com/boto/boto3/issues/2424
        read_timeout=900, connect_timeout=900, retries={"max_attempts": 0}
    )
    lambda_client = boto3.client("lambda", config=config)
    response = lambda_client.invoke(
        FunctionName="authors-homogenization-dev-insert-book",
        InvocationType="RequestResponse",
        Payload=json.dumps({"source": source, "books": books}),
    )
    assert response["StatusCode"] < 300


with Flow("BibliotecaBooks") as flow:
    biblioteca_raw_books = get_books_biblioteca()
    biblioteca_clean_books = clean_books_biblioteca.map(
        biblioteca_raw_books,
        upstream_tasks=[biblioteca_raw_books],
    )
    books_batches = create_books_batches(
        biblioteca_clean_books,
        upstream_tasks=[biblioteca_clean_books],
    )
    # for batch in books_batches:
    #     insert_books = insert_books(batch, source='biblioteca', upstream_tasks=[books_batches])
    insert_biblioteca_books = insert_books.map(
        books_batches,
        source=unmapped("biblioteca"),
        upstream_tasks=[books_batches],
    )

flow.executor = LocalDaskExecutor()
flow.storage = GitHub(
    repo="bishetheanswer/trial-docker",
    path="flows/biblioteca_flow.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
