import json

import pendulum

from airflow.decorators import dag, task

@dag(
    dag_id="extract_csv_data",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["extract"],
)

def extract_job_skils_taskflow_api():
    @task()
    def import_csv_to_db():
        import pandas as pd
        import boto3
        from botocore.exceptions import ClientError

        csv_path = '/home/vagrant/airflow/job_skills.csv'

        client = boto3.client(
            's3',
            aws_access_key_id='accessKey1',
            aws_secret_access_key='verySecretKey1',
            endpoint_url='http://10.0.0.30:8000/'
        )

        bucket_name = 'csvrawchunks'

        client.create_bucket(Bucket=bucket_name)

        def upload_s3_file(file_name, bucket, object_name=None):
            if object_name is None:
                object_name = os.path.basename(file_name)

            # Upload the file
            s3_client = boto3.client('s3')
            try:
                response = client.upload_file(file_name, bucket, object_name)
            except ClientError as e:
                print(e)
                return False
            return True


        
        chunksize = 100
        batch_no = 1
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            csv_batch_name = 'job_skills_chunk_{}.csv'.format(batch_no)
            chunk.to_csv(csv_batch_name, index=False)
            upload_s3_file(csv_batch_name, bucket_name)
            batch_no +=1

        

    job_skills = import_csv_to_db()

extract_job_skils_taskflow_api()
