from prefect.storage import GitHub
from prefect import task, Flow
import boto3
import botocore
import json
from prefect.executors import LocalDaskExecutor
from datetime import timedelta
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock


@task(name="GetBooksIt")
def get_books_itbooks():
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName="authors-homogenization-dev-get-books-itbooks",
        InvocationType="RequestResponse",
    )
    assert response["StatusCode"] < 300  # TODO change
    payload = json.loads(response["Payload"].read())
    return payload


@task(name="GetBooksNYTimes")
def get_books_nytimes():
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName="authors-homogenization-dev-get-books-nytimes",
        InvocationType="RequestResponse",
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
        FunctionName="authors-homogenization-dev-insert-book",
        InvocationType="RequestResponse",
        Payload=json.dumps({"source": source, "books": books}),
    )
    assert response["StatusCode"] < 300


schedule = Schedule(clocks=[IntervalClock(timedelta(hours=24))])
with Flow("ApiBooks", schedule=schedule) as flow:
    it_books = get_books_itbooks()
    nytimes_books = get_books_nytimes()
    insert_it_books = insert_books(
        it_books, source="itbooks-api", upstream_tasks=[it_books]
    )
    insert_nytimes_books = insert_books(
        nytimes_books, source="nytimes-api", upstream_tasks=[nytimes_books]
    )

flow.executor = LocalDaskExecutor()
flow.storage = GitHub(
    repo="bishetheanswer/trial-docker",
    path="flows/apis_flow.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
