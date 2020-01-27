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



"""
Example Queue Notification:

[
    {
        'Id': 'string',
        'QueueArn': 'string',       #should by 'LambdaFunctionArn' for Lambda notification or 'TopicArn' for SNS notification.
        'Events': [
            's3:ReducedRedundancyLostObject'|'s3:ObjectCreated:*'|'s3:ObjectCreated:Put'|'s3:ObjectCreated:Post'|'s3:ObjectCreated:Copy'|'s3:ObjectCreated:CompleteMultipartUpload'|'s3:ObjectRemoved:*'|'s3:ObjectRemoved:Delete'|'s3:ObjectRemoved:DeleteMarkerCreated'|'s3:ObjectRestore:*'|'s3:ObjectRestore:Post'|'s3:ObjectRestore:Completed'|'s3:Replication:*'|'s3:Replication:OperationFailedReplication'|'s3:Replication:OperationNotTracked'|'s3:Replication:OperationMissedThreshold'|'s3:Replication:OperationReplicatedAfterThreshold',
        ],
        'Filter': {
            'Key': {
                'FilterRules': [
                    {
                        'Name': 'prefix'|'suffix',
                        'Value': 'string'
                    },
                ]
            }
        }
    }
]

"""