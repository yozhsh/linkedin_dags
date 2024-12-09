
def s3client():
    import boto3

    client = boto3.client(
            's3',
            aws_access_key_id='accessKey1',
            aws_secret_access_key='verySecretKey1',
            endpoint_url='http://10.0.0.30:8000/'
        )
    return client

def upload_s3_file(s3client, file_name, bucket, object_name=None):
            import os
            from boto3.exceptions import ClientError

            if object_name is None:
                object_name = os.path.basename(file_name)

            # Upload the file
            try:
                response = s3client.upload_file(file_name, bucket, object_name)
            except ClientError as e:
                print(e)
                return False
            return True
