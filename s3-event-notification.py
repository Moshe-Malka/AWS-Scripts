import sys
import boto3

bucket_name = '<FILL_THIS>'

s3_resource = boto3.resource('s3')

def set_queue_notification(conf):
    try:
        response = s3_resource.BucketNotification(bucket_name).put(NotificationConfiguration={'QueueConfigurations' : conf})
        print(f"Successfully set queue notification : {response}")
    except Exception as e:
        print(f"Failed to set queue notification : {e}")


def set_topic_notification(conf):
    try:
        response = s3_resource.BucketNotification(bucket_name).put(NotificationConfiguration={'TopicConfigurations' : conf})
        print(f"Successfully set topic notification : {response}")
    except Exception as e:
        print(f"Failed to set topic notification : {e}")


def set_lambda_notification(conf):
    try:
        response = s3_resource.BucketNotification(bucket_name).put(NotificationConfiguration={'LambdaFunctionConfigurations' : conf})
        print(f"Successfully set lambda notification : {response}")
    except Exception as e:
        print(f"Failed to set lambda notification : {e}")


def unset_notification(_id, _type):
    try:
        bucket_notification = s3_resource.BucketNotification(bucket_name)
        if _type == 'queue':
            configs = bucket_notification.queue_configurations
        elif _type == 'topic':
            configs = bucket_notification.topic_configurations
        elif _type == 'lambda':
            configs = bucket_notification.lambda_function_configurations
        else:
            print(f"Error - not a valid type - <{_type}>")
            sys.exit(1)
        new_configs = [ x for x in configs if x['Id'] !== _id]
        set_queue_notification(new_configs)
        bucket_notification.load()
    except Exception as e:
        print(f"Failed to unset queue notification : {e}")
