
import sys, os, time, re, uuid, json
from datetime import datetime
from urllib.parse import unquote_plus

from boto3.session import Session
import textract

RAW_FILES_QUEUE_NAME = 'raw-files-queue.fifo'

def get_session():
    try:
        return Session(region_name='us-west-2')
    except Exception as e:
        print(f"[getSession] {e}")
        sys.exit(1)

def read_messages_from_queue(q_name):
    try:
        queue = sqs_client.get_queue_url( QueueName=q_name )
        response = sqs_client.receive_message( QueueUrl=queue['QueueUrl'],AttributeNames=['All'], WaitTimeSeconds=10, MaxNumberOfMessages=1 )
        if 'Messages' not in response:
            print(f"<{datetime.now().isoformat()}> No Messages in queue.")
            return (None, [])

        print(f"<{datetime.now().isoformat()}> Messages Pulled : {len(response['Messages'])}")
        return (queue['QueueUrl'], response['Messages'])
    except Exception as e:
        print(f"<{datetime.now().isoformat()}> {e}")
        return (None, [])

def download_raw_file(bucket, key):
    try:
        if '/' in key:
            folders = '/'.join(key.split('/')[:-1])
            try:
                if not os.path.exists(f"/tmp/{folders}"):
                    os.makedirs(f"/tmp/{folders}")
            except FileExistsError as e:
                print(f"<{datetime.now().isoformat()}> [download_raw_file] {e}")
                return None
        local_filename = f"/tmp/{unquote_plus(key)}"
        print(f"<{datetime.now().isoformat()}> Started downloading {local_filename}")
        try:
            down_response = s3_client.download_file(bucket, unquote_plus(key), local_filename)
        except Exception as e:
            print(f"Error While Downloading - {e}")
        return local_filename
    except Exception as e:
        print(f"<{datetime.now().isoformat()}> [download_raw_file] {e}")
        return None

def extract_text(fname):
    try:
        raw_content = textract.process(fname, encoding="utf-8")
        formatted_content = re.sub(r"\s+", " ",raw_content.decode("utf-8"))
        return formatted_content
    except Exception as e:
        print(f"<{datetime.now().isoformat()}> [extract_text] {e}")
        return None

def upload_converted_file(bucket, string_data, new_key, org_key):
    converted_files_bucket = 'converted-files'
    content_type="text/plain;charset=utf-8"
    try:
        data = str.encode(string_data)
        upload_response = s3_client.put_object(
            Body=data,
            Bucket=converted_files_bucket,
            Key=new_key,
            ContentType=content_type,
            Tagging=f"OriginalFileExtension={org_key.split('.')[-1]}"
            )
        return upload_response
    except Exception as e:
        print(f"<{datetime.now().isoformat()}> [upload_converted_file] {e}")
        return None

def delete_message_from_queue(q_url, receipt):
    try:
        delete_response = sqs_client.delete_message(QueueUrl=q_url, ReceiptHandle=receipt)
        if delete_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"<{datetime.now().isoformat()}> Deleted Object from queue")
        else:
            print(f"<{datetime.now().isoformat()}> Failed to Delete Receipt {receipt}")
    except Exception as e:
        print(f"<{datetime.now().isoformat()}> [delete_message_from_queue] - {e}")

def delete_tmp_raw_file(filename):
    try:
        os.remove(filename)
        print(f"<{datetime.now().isoformat()}> Deleted file <{filename}> from /tmp folder.")
    except OSError as ose:
        print(f"<{datetime.now().isoformat()}> Failed to remove file <{filename}> from /tmp folder.")

if __name__ == '__main__':
    print(f"{'*'*55} Started Extractor App {'*'*55}")

    sess = get_session()
    sqs_client = sess.client('sqs')
    s3_client = sess.client('s3')
    try:
        while True:
            q_url, messages = read_messages_from_queue(RAW_FILES_QUEUE_NAME)
            for message in messages:
                print(f"<{datetime.now().isoformat()}> Message Id : {message['MessageId']}")
                # ################ extracting message content ################
                body = json.loads(message['Body'])
                receipt = message['ReceiptHandle']

                bucket_name = body['bucket_name']
                key = body['key']
                # ################ downloading raw file from s3 ################
                local_filename = download_raw_file(bucket_name, key)
                if not local_filename:
                    print(f"<{datetime.now().isoformat()}> Failed to download {key} from bucket {bucket_name}.")
                    delete_message_from_queue(q_url, receipt)
                    print(f"<{datetime.now().isoformat()}> {'-'*50}")
                    continue
                print(f"<{datetime.now().isoformat()}> Finished downloading file <{local_filename}>")
                # ################ extracting file text content ################
                start = time.time()
                extracted = extract_text(local_filename)
                print(f"<{datetime.now().isoformat()}> Extraction Took {(time.time() - start)}")
                if not extracted:
                    print(f"<{datetime.now().isoformat()}> No extracted text.")
                    delete_message_from_queue(q_url, receipt)
                    delete_tmp_raw_file(local_filename)
                    print(f"<{datetime.now().isoformat()}> {'-'*50}")
                    continue
                # ################ uploading extracted file content ################
                new_key = f"{'.'.join(key.split('.')[:-1])}.txt"
                response = upload_converted_file(bucket_name, extracted, new_key, key)
                if not response: print(f"<{datetime.now().isoformat()}> Failed to push new converted file.")
                else:
                    print(f"<{datetime.now().isoformat()}> Successfully Processed file {unquote_plus(key)} from bucket {bucket_name}")
                    delete_message_from_queue(q_url, receipt)
                delete_tmp_raw_file(local_filename)
                print(f"<{datetime.now().isoformat()}> {'-'*50}")
    except KeyboardInterrupt as ki:
        print("Exiting...")
        sys.exit(1)
    except Exception as e:
        print(f"Generic Error - {e}")
        sys.exit(1)