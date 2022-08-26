import json


# import json
# import boto3
# import time
# from boto3.dynamodb.types import TypeDeserializer

# # Nos creamos una función para manejar de forma más cómoda los datos de Dynamo

# def deserialize(item):
#     deserializer = TypeDeserializer()
#     return {k: deserializer.deserialize(v) for k, v in item.items()}

# def lambda_handler(event, context):    
#     counter = 0
#     min_fecha_entrada = int(time.time())
#     for record in event.get('Records', []):
#         record_dynamo = record.get('dynamodb')
#         if record_dynamo:            
#             claves = record_dynamo["Keys"]
#             nueva_entrada = record_dynamo["NewImage"]
#             fecha_entrada = int(record_dynamo["ApproximateCreationDateTime"])      
#             min_fecha_entrada = min(min_fecha_entrada, fecha_entrada)
            
#             print(f'Fecha entrada: {fecha_entrada}') # También la guardamos en el registro
#             print(f'Nueva entrada: {deserialize(nueva_entrada)}\n')
            
#             counter = counter+1
    
    
#     dif_time = int(time.time()) - min_fecha_entrada
#     print(f'{counter} entradas procesadas con una edad de {dif_time} segundos ({dif_time/60} minutos)')            
import os
import boto3

DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")


def handler(event, context):
    dynamo_client = boto3.client("dynamodb", region_name="us-east-1")
    # print("Updating tableeee")
    # print(event)
    for record in event.get('Records', []):
        record_dynamo = record.get('dynamodb')
        if record_dynamo:
            keys = record_dynamo["Keys"]
            author = keys['author']

            CONDICION = 'ADD n_books :new_book'
            ATRIBUTOS_CONDICION = {':new_book': {'N': '1'}}
            dynamo_client.update_item(
                TableName=DYNAMO_TABLE,
                Key={
                    'author': author,
                },
                UpdateExpression=CONDICION,
                ExpressionAttributeValues=ATRIBUTOS_CONDICION
            )
            print(author)

    return {"statusCode": 200}