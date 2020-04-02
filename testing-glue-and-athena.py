import boto3
import time
from pprint import pprint

glue_client = boto3.client('glue', region_name='us-west-2')
athena_client = boto3.client('athena', region_name='us-west-2')

crawler_role_arn = 'arn:aws:iam::458558152824:role/service-role/AWSGlueServiceRole-crawler_1'
database_name = 'testing_glue'
raw_bucket = "raw-trip-data-allcloud/test_folder1/"
parquet_bucket = "parquet-trip-data-allcloud/test_folder1/"
query_results_bucket = 's3://query-results-trip-data-allcloud/test-results/'

def create_crawler(name):
    try:
        glue_client.create_crawler(Name=name, Role=crawler_role_arn, DatabaseName=database_name, Targets={'S3Targets' : [{'Path': raw_bucket}]})
        return True
    except Exception as e:
        print(f"Failed to create new crawler with name {name}.")
        return False

def crawler_exists(name):
    try:
        glue_client.get_crawler(Name=name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Crawler <{name}> Not Found -> creating a new one.")
        return False
        
def run_crawler(name):
    try:
        glue_client.start_crawler(Name=name)
        print(f"Succefully Started Crawler <{name}>")
    except glue_client.exceptions.CrawlerRunningException:
        print(f"Crawler <{name}> already running")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Crawler <{name}> Not Found")

def create_database(db_name):
    try:
        glue_client.create_database(DatabaseInput={'Name': db_name})
        print(f"Successfully created new database with name <{db_name}>")
    except Exception as e:
        print(f"Failed to create new database with name <{db_name}> : {e}")

def create_job(job_name, job_role, command):
    """ command = 
    {
        'Name' : "abc",
        'ScriptLocation' : 's3://<bucket>/<folder>/<script>',
        'PythonVersion' : '3'
    }"""
    try:
        glue_client.create_job(Name=job_name, Role=job_role, Command=command)
    except Exception as e:
        peinr(e)

def run_query(query_string):
    try:
        response = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={ 'Database': database_name },
            ResultConfiguration={ 'OutputLocation': query_results_bucket },
        )
        return response['QueryExecutionId']
    except Exception as e:
        print(e)
        return None

def get_query_results(query_id):
    try:
        response = athena_client.get_query_results(QueryExecutionId=query_id)
        return response['ResultSet']
    except athena_client.exceptions.InvalidRequestException as invalid_req:
        print(invalid_req)
    except Exception as e:
        print(e)
        return None

def get_crawler_state(crawler_name):
    try:
        return glue_client.get_crawler(crawler_name)['Crawler']['State']
    except Exception as e:
        print(e)
        return None

def get_query_state(exacution_id):
    try:
        status = athena_client.get_query_execution(QueryExecutionId=exacution_id)['QueryExecution']['Status']
        state = status['State']
        if state == 'FAILED': print(status['StateChangeReason'])
        return state
    except Exception as e:
        return 'FAILED'
        print(e)

def csv_to_parquet_athena():
    try:
        table_name = raw_bucket.split('/')[-2]
        query = f"""
            CREATE TABLE "testing_glue"."par_{table_name}"
            WITH (
                format = 'PARQUET',
                parquet_compression = 'SNAPPY',
                external_location = 's3://{parquet_bucket}'
            ) AS SELECT * FROM "testing_glue"."{table_name}"
        """
        return run_query(query)
    except Exception as e:
        print(e)

query = """ """
exacution_id = run_query(query)
while True:
    curr_query_state = get_query_state(exacution_id)
    print(f"Query State : {curr_query_state}")
    if curr_query_state == 'FAILED': break
    if curr_query_state == 'SUCCEEDED':
        print("Finished")
        query_results = get_query_results(exacution_id)
        pprint(query_results)
        break
    else:
        time.sleep(5)


######################### Converting existing table to parquet format #########################
# ex_id = csv_to_parquet()

# while True:
#     curr_query_state = get_query_state(ex_id)
#     print(f"Query State : {curr_query_state}")
#     if curr_query_state == 'FAILED': break
#     if curr_query_state == 'SUCCEEDED':
#         print("Finished")
#         query_results = get_query_results(ex_id)
#         pprint(query_results)
#         break
#     else:
#         time.sleep(5)

######################### Starting, creating and running a crawler #########################
# crawler_name = 'test_crawler'

# if not crawler_exists(crawler_name):
#     create_database(database_name)
#     create_crawler(crawler_name)
#     run_crawler(crawler_name)
    
#     while True:
#         current_state = get_crawler_state(crawler_name)
#         print(f"Crawler State : {current_state}")
#         if current_state == 'READY': break
#         else:
#             time.sleep(5)
# else:
#     print(f"Crawler with name {crawler_name} already exists.")






# 1) 