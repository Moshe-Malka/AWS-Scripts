import boto3
from pprint import pprint
import time
import json

s3_client = boto3.client('s3')

start = time.time()
response = s3_client.select_object_content(
    Bucket='raw-files-dropsite',
    Key='trip_data_8.csv',
    Expression='SELECT COUNT(*) FROM s3object',
    ExpressionType='SQL',
    InputSerialization={
        "CSV": {
            'FileHeaderInfo': 'USE',
        },
    },
    OutputSerialization={
        'JSON': {}
    }
)

# data = [x for x in response['Payload']]
# pprint(data)
pprint(json.loads([x['Records']['Payload'].decode('utf-8') for x in response['Payload'] if 'Records' in  x][0])['_1'])
print(f"SELECT Took {(time.time()-start):.3f} Seconds")

print('#'*50)

start = time.time()
print( len(s3_client.get_object(Bucket='raw-files-dropsite', Key='trip_data_8.csv')['Body']._raw_stream.readlines()[1:]) )
print(f"READLINES Took {(time.time()-start):.3f} Seconds")