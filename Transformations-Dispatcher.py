import os, json
from uuid import uuid4
import boto3
from botocore.exceptions import ClientError

sf_client = boto3.client('stepfunctions')
s3_resource = boto3.resource('s3')
athena_client = boto3.client('athena')

def start_sf_exacution(payload):
    try:
        response = sf_client.start_execution(
            stateMachineArn=os.environ['SF_ARN'],
            name=f'Tranformation-Run_{uuid4()}',
            input=json.dumps(payload))
        print("Successfully started step function run")
        print(f"Response : {response}")
    except ClientError as ce:
        print(f"Failed to invoke Step Function : {ce}")

def set_s3_lambda_notification(conf):
    try:
        response = s3_resource.BucketNotification(os.environ['RESULTS_BUCKET_NAME']).put(NotificationConfiguration={'LambdaFunctionConfigurations' : conf})
        print(f"Successfully set queue notification : {response}")
    except Exception as e:
        print(f"Failed to set S3 notification: {e}")
        
def start_query_exacution(query_str, folder_id):
    try:
        response = athena_client.start_query_execution(
            QueryString=query_str,
            QueryExecutionContext={
                'Database': os.environ['DB_NAME']
            },
            ResultConfiguration={
                'OutputLocation': f"s3://{os.environ['RESULTS_BUCKET_NAME']}/Query-Results/Athena-Query/{folder_id}/"
            })
        print(f"Successfully ran  : {response}")
    except ClientError as ce:
        print(f"Failed to start query exacution : {ce}")
    except Exception as e:
        print(f"Failed to start query exacution : {e}")

def lambda_handler(event, context):
    print(f"Event : {event}")
    for record in event['Records']:
        message_body = json.loads(record['body'])
        # Option 1: Invoke Step Function
        start_sf_exacution(json.dumps({
            'tables' : message_body['files_done'],
            'dump_path' : f"s3://{os.environ['RESULTS_BUCKET_NAME']}/Query-Results/Glue-Job/{folder_id}/"
        }))
    
        # Option 2: set s3 notification and run query
        notification_id = str(uuid4())
        set_s3_lambda_notification([{
            'Id': notification_id,
            'LambdaFunctionArn': os.environ['LAMBDA_ARN'],
            'Events': ['s3:ObjectCreated:*'],
            'Filter': {
                'Key': {
                    'FilterRules': [
                        {
                            'Name': 'prefix',
                            'Value': f'Query-Results/Athena-Query/{notification_id}/'
                        }
                    ]
                }
            }
        }])
        query_str = """ SELECT * FROM MASHO """
        start_query_exacution(query_str, notification_id)
    
    
    
