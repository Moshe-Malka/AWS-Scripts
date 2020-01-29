import os
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('MyTable')

def put_item(key, value):
    """
    Put an item into our table.
    """
    try:
        response = table.put_item( Item={ 'my-key': key, 'some-other-key': value )
        print(f"Successfully added new item")
        print(f"Response : {response}")
    except ClientError as ce:
        print(f"Failed to creat new item - key : {key}, value : {value}")
        print(ce)

def update_nested_item(key, value):
    """
    Update a nested item. create 
    """
    try:
        response = table.update_item( Key={ 'my-key': key },
            UpdateExpression='SET #other-key = :new_value',
            ExpressionAttributeNames={
                '#other-key': 'New-Key'
            },
            ExpressionAttributeValues={ ':new_value': True },
            ReturnValues='ALL_NEW'
        )
        print("Successfully created/updated item.")
        print(f"Response : {response}")
    except ClientError as ce:
        print(f"Failed to update item : {ce}")
    