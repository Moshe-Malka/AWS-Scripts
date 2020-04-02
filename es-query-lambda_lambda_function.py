import boto3
import os
import json

from elasticsearch_dsl import Search, Q
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

credentials = boto3.Session().get_credentials()
es_host = os.environ['ES_ENDPOINT']

awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, os.environ['REGION'], 'es', session_token=credentials.token)

es = Elasticsearch(
    hosts=[{'host': es_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    retry_on_timeout=True,
    timeout=10000,
    http_compress=True
)

s = Search(using=es, index="tj-files")\
    .highlight('object_body', fragment_size=50, fragment_offset=5)\
    .sort('_score')\
    .extra(size=50)

def lambda_handler(event, context):
    print(f"Event : {event}")
    keyword = event['keyword']
    
    # Using Elasticsearch-DSL
    response = s.query("query_string", query=f"*{keyword}*", fields=['object_body'], analyze_wildcard=True, rewrite="scoring_boolean").execute()

    print(f"Response Hits Count : {response.hits.total.value}")
    
    if response.success() and response.hits.total.value > 0:
        print(f"Response : {response.to_dict()}")
        return {
            'statusCode' : 200,
            'message' : 'Success',
            'query_results_count' : response.hits.total.value,
            'query_took' : response.took,
            'query_results' :
                [ {
                    "id" : hit.meta.id,
                    "highlights" : [{'key' : k, 'value' : v} for (k,v) in zip(range(1, len(hit.meta.highlight.object_body)+1), hit.meta.highlight.object_body)],
                    "filename" : '.'.join(hit.filename.split('.')[:-1]),
                    "extension" : hit.original_extension if 'original_extension' in hit else 'None',
                    "score" : hit.meta.score,
                    "size" : hit.object_size
                    } for hit in response] }
    else:
        print(f"[Error] Query for keyword <{keyword}> Failed.")
        return {
            'statusCode' : 500,
            'message' : f"Query for keyword <{keyword}> Failed.",
            'query_results' : []
        }
    
    # ##### Document Fields #####
    # "filename"
    # "object_body" 
    # "object_size" 
    # "upload_date"
    # "upload_time"
    # "original_extension"
    