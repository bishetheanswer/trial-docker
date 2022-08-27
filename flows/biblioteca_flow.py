import prefect
from prefect.storage import GitHub
from prefect import task, Flow, unmapped
import boto3
import botocore
import datetime
import json


@task(name="GetBooksBiblioteca")
def get_books_biblioteca():
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName="trial-docker-dev-get-books-biblioteca-nacional",
        InvocationType="RequestResponse",
    )
    assert response["StatusCode"] < 300
    payload = json.loads(response["Payload"].read())
    return payload[0]  # TODO return only first because of testing purposes


@task(name="CleanBooksBiblioteca")
def clean_books_biblioteca(csv_key):
    config = botocore.config.Config(  # https://github.com/boto/boto3/issues/2424
        read_timeout=900, connect_timeout=900, retries={"max_attempts": 0}
    )
    lambda_client = boto3.client("lambda", config=config)
    response = lambda_client.invoke(
        FunctionName="trial-docker-dev-clean-books-biblioteca",
        InvocationType="RequestResponse",
        Payload=json.dumps({"csv_key": csv_key}),
    )
    assert response["StatusCode"] < 300
    payload = json.loads(response["Payload"].read())
    return payload


@task(name="InsertBooks")
def insert_books(books, source):
    config = botocore.config.Config(  # https://github.com/boto/boto3/issues/2424
        read_timeout=900, connect_timeout=900, retries={"max_attempts": 0}
    )
    lambda_client = boto3.client("lambda", config=config)
    response = lambda_client.invoke(
        FunctionName="trial-docker-dev-insert-book",
        InvocationType="RequestResponse",
        Payload=json.dumps({"source": source, "books": books}),
    )
    assert response["StatusCode"] < 300


with Flow("BibliotecaBooks") as flow:
    biblioteca_raw_books = get_books_biblioteca()
    biblioteca_clean_books = clean_books_biblioteca(
        biblioteca_raw_books, 
        upstream_tasks=[biblioteca_raw_books],
    )
    insert_biblioteca_books = insert_books(
        biblioteca_clean_books,
        source="biblioteca",
        upstream_tasks=[biblioteca_clean_books],
    )

flow.storage = GitHub(
    repo=" bishetheanswer/trial-docker",
    path="flows/biblioteca_flow.py",
    access_token_secret="GITHUB_ACCESS_TOKEN"
)