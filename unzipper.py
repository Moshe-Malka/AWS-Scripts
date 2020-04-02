import sys, json, io, zipfile, time
import boto3

def main():
    """
    get the sources via the arguments that are passed to the glue job, get the zip file from s3, unzip and dump the result to s3.
    """
    args = {
        'source_bucket' : 'testing-ingestion-pipeline',
        'source_key' : 'raw/tripdata_fhv_2015.zip',
        'destination_bucket' : 'testing-ingestion-pipeline' 
    }

    source_bucket = args["source_bucket"]
    source_key = args["source_key"]
    destination_bucket = args["destination_bucket"]
    
    start = time.time()

    # get zip file from s3
    s3_object = boto3.resource('s3').Object(bucket_name=source_bucket, key=source_key)
    zip_file_byte_object = io.BytesIO(s3_object.get()["Body"].read())
    zip_file = zipfile.ZipFile(zip_file_byte_object)
    name_list = [x for x in zip_file.namelist() if '.csv' in x]
    
    print(f"Got Zip File from bucket <{source_bucket}> and key <{source_key}>")
    print(f"Zip Download Took : {(time.time()-start):.3f} Seconds.")

    processed = []
    start = time.time()
    # unzip the zip file and write contents to s3
    for file_path in name_list:
        print(f'processing email path {file_path}')
        with zip_file.open(file_path) as f:
            print(f"File Path : {file_path}")
            file_byte_object = io.BytesIO(f.read())
            top_folder = '.'.join(source_key.split('/')[-1].split('.')[:-1])
            full_destination_key = f"staging/{top_folder}/{file_path.split('/')[-1]}"
            print(f'Started processing - {full_destination_key}')
            boto3.client('s3').upload_fileobj(file_byte_object, destination_bucket, full_destination_key)
            processed.append(full_destination_key)
            print(f"Finished processing - {full_destination_key}")
    
    print(f'Finished Unziping {source_bucket}/{source_key}')
    print(f"Files upload Download Took : {(time.time()-start):.3f} Seconds.")
    message = { 'message_source': 'unziper-script', 'args': args, 'processed': processed, 'crawler_destination': '/'.join(processed[0].split('/')[:-2]) }
    print(f"Message : {message}")
    return "Done"

if __name__ == '__main__':
    main()
