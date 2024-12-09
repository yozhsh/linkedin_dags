import json

import pendulum
import os

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



def s3client():
    import boto3

    client = boto3.resource(
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


@dag(
    dag_id="LinkedinETL",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["extract"],
)
def etl():

    # create_table_skills = SQLExecuteQueryOperator(
    #         task_id="create_table_skills",
    #         sql="create_skills.sql",
    #         return_last=False
    #     )
    

    @task()
    def extract_jobskills_chunks_to_s3():
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

        bucket_name = 'jobskillchunks'

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
            os.remove('/home/vagrant/airflow/job_skills_chunk_{}.csv'.format(batch_no))
            batch_no +=1

    @task()
    def extract_jobposting_chunks_to_s3():
        import pandas as pd
        import boto3
        from botocore.exceptions import ClientError

        csv_path = '/home/vagrant/airflow/linkedin_job_postings.csv'

        client = boto3.client(
            's3',
            aws_access_key_id='accessKey1',
            aws_secret_access_key='verySecretKey1',
            endpoint_url='http://10.0.0.30:8000/'
        )

        bucket_name = 'jobpostingchunks'

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
            csv_batch_name = 'jobposting_chunk_{}.csv'.format(batch_no)
            chunk.to_csv(csv_batch_name, index=False)
            upload_s3_file(csv_batch_name, bucket_name)
            os.remove('/home/vagrant/airflow/jobposting_chunk_{}.csv'.format(batch_no))
            batch_no +=1
        
    @task()
    def extract_job_summary_chunks_to_s3():
        import pandas as pd
        import boto3
        from botocore.exceptions import ClientError
        import csv

        csv_path = '/home/vagrant/airflow/job_summary.csv'

        client = boto3.client(
            's3',
            aws_access_key_id='accessKey1',
            aws_secret_access_key='verySecretKey1',
            endpoint_url='http://10.0.0.30:8000/'
        )

        bucket_name = 'jobsummarychunks'

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
        #skiprows

    
        for chunk in pd.read_csv(csv_path, chunksize=chunksize, on_bad_lines='skip', skiprows=[857065]):
            csv_batch_name = 'jobsummary_chunk_{}.csv'.format(batch_no)
            chunk.to_csv(csv_batch_name, index=False)
            upload_s3_file(csv_batch_name, bucket_name)
            os.remove('/home/vagrant/airflow/jobsummary_chunk_{}.csv'.format(batch_no))
            batch_no +=1

    

    @task()
    def transform_jobskills_chunk_to_db():
        import pandas as pd

        s3 = s3client()
        bucket = s3.Bucket('jobskillchunks')

        for s3_obj in bucket.objects.all():
            filename = s3_obj.key
            download_path = '/tmp/{}'.format(filename)
            bucket.download_file(filename, download_path)
            df = pd.read_csv(download_path)
            job_skills = df.get('job_skills').to_string()
            print(job_skills)










    # extract_jobskills_chunks_to_s3() >> create_table_skills >> transform_jobskills_chunk_to_db() 
    # extract_jobposting_chunks_to_s3()
    # extract_job_summary_chunks_to_s3()

    transform_jobskills_chunk_to_db()
    
    

etl()

