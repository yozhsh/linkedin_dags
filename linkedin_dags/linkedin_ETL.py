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


def parse_string(csv_string):
    return csv_string.split(",")

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
    def prepare_db_to_jobskill_transform():
        import psycopg2

        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS skills (
                id SERIAL PRIMARY KEY,
                name VARCHAR (255) NOT NULL UNIQUE
            );'''
        )
        
        dbclient.commit()
        cursor.close()
        dbclient.close()


    @task()
    def transform_jobskills_chunk_to_db():
        import pandas as pd
        import psycopg2
        from psycopg2.extras import execute_values
        from psycopg2 import errors
        from psycopg2.errorcodes import UNIQUE_VIOLATION, IN_FAILED_SQL_TRANSACTION, STRING_DATA_RIGHT_TRUNCATION



        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()

        s3 = s3client()
        bucket = s3.Bucket('jobskillchunks')

        for s3_obj in bucket.objects.all():
            filename = s3_obj.key
            download_path = '/tmp/{}'.format(filename)
            bucket.download_file(filename, download_path)
            df = pd.read_csv(download_path)
            job_skills = df.get('job_skills').to_dict()
            try:
                lst_of_str = parse_string(job_skills[0])
            except AttributeError:
                continue

            for skill in lst_of_str:
                try:
                    cursor.execute(
                        "INSERT INTO skills (name) VALUES (%s)",
                        (skill.lstrip(),))
                    cursor.execute("commit")
                except errors.lookup(IN_FAILED_SQL_TRANSACTION):
                    cursor.execute("rollback")
                    cursor.execute(
                        "INSERT INTO skills (name) VALUES (%s)",
                        (skill.lstrip(),))
                    cursor.execute("commit")    
                except errors.lookup(UNIQUE_VIOLATION):
                    cursor.execute("rollback")
                    continue
                except errors.lookup(STRING_DATA_RIGHT_TRUNCATION):
                    cursor.execute("rollback")
                    continue
        cursor.close()
        dbclient.close()

    @task()
    def prepare_db_to_jobposting():
        import psycopg2

        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()


        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS job (
                id SERIAL PRIMARY KEY,
                title VARCHAR (255),
                link VARCHAR (500) UNIQUE,
                location VARCHAR (255),
                search_city VARCHAR (255),
                level VARCHAR(255),
                type VARCHAR (255),
                got_summary BOOLEAN NOT NULL,
                summary_id INTEGER REFERENCES summary,
                company_id INTEGER REFERENCES company 
            )'''
        )
        dbclient.commit()
        cursor.close()
        dbclient.close()

    # @task()
    # def transform_jobposting_chunk_to_db():
    #     import pandas as pd
    #     import psycopg2

    #     dbclient = psycopg2.connect(
    #         database='etl_raw_data',
    #         user='airflow_user',
    #         password='eserloqpbeq',
    #         host='10.0.0.20'
    #         )
        
    #     cursor = dbclient.cursor()

    #     s3 = s3client()
    #     bucket = s3.Bucket('jobpostingchunks')

    #     for s3_obj in bucket.objects.all():
    #         filename = s3_obj.key
    #         download_path = '/tmp/{}'.format(filename)
    #         bucket.download_file(filename, download_path)
    #         df = pd.read_csv(download_path)
    #         print("TO_DICT____________")
    #         obj = df.get(['job_title', 'job_link', 'job_location', 'search_city', 
    #                       'job_level', 'job_type', 'company'])
            
    #         dicted = obj.to_dict()
    #         for column in dicted:
    #             for content in dicted[column]:
    #                 print(dicted[column][content])
    #                 try:
    #                     cursor.execute(
    #                         "INSERT INTO job (title, link, location, search_city, level, type, company_id)"
    #                     )
    #         break
            
        cursor.close()
        dbclient.close()
            
    @task()
    def prepare_db_to_joblink():
        import psycopg2

        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS joblink (
                id SERIAL PRIMARY KEY,
                link VARCHAR (500) NOT NULL UNIQUE
            )'''
        )
        dbclient.commit()
        cursor.close()
        dbclient.close()
    
    @task()
    def prepare_buckets_to_joblink():
        s3 = s3client()
        s3.create_bucket(Bucket='joblink')
    
    @task()
    def extract_joblink_from_jobskill_to_db():
        import pandas as pd
        import json

        s3 = s3client()
        bucket = s3.Bucket('jobskillchunks')

        job_links = []
        for s3_obj in bucket.objects.all():
            filename = s3_obj.key
            download_path = '/tmp/{}'.format(filename)
            bucket.download_file(filename, download_path)
            df = pd.read_csv(download_path)
            joblink = df.get('job_link')
            links = joblink.to_list()
            job_links += links
            break
            
        data = {'job_links': job_links}
        json_data = json.dumps(data).encode('UTF-8')
        json_fname = 'joblink_1.json'
        s3_Obj = s3.Object('joblink', json_fname)
        s3_Obj.put(
            Body=(bytes(json_data))
        )
        return json_fname
    

    @task()
    def extract_joblink_from_jobposting_to_db():
        import pandas as pd

        s3 = s3client()

        job_links = []
        bucket = s3.Bucket('jobpostingchunks')
        for s3_obj in bucket.objects.all():
            filename = s3_obj.key
            download_path = '/tmp/{}'.format(filename)
            bucket.download_file(filename, download_path)
            df = pd.read_csv(download_path)
            joblink = df.get('job_link')
            links = joblink.to_list()
            job_links += links
            break
        
        data = {'job_links': job_links}
        json_data = json.dumps(data).encode('UTF-8')
        json_fname = 'joblink_2.json'
        s3_Obj = s3.Object('joblink', json_fname)
        s3_Obj.put(
            Body=(bytes(json_data))
        )
        return json_fname
    

    @task()
    def extract_joblink_from_jobsummary_to_db():
        import pandas as pd


        s3 = s3client()
        bucket = s3.Bucket('jobsummarychunks')

        job_links = []
        for s3_obj in bucket.objects.all():
            filename = s3_obj.key
            download_path = '/tmp/{}'.format(filename)
            bucket.download_file(filename, download_path)
            df = pd.read_csv(download_path)
            joblink = df.get('job_link')
            links = joblink.to_list()
            job_links += links
            break

        data = {'job_links': job_links}
        json_data = json.dumps(data).encode('UTF-8')
        json_fname = 'joblink_3.json'
        s3_Obj = s3.Object('joblink', json_fname)
        s3_Obj.put(
            Body=(bytes(json_data))
        )
        return json_fname        
    

    @task()
    def insert_joblink_to_db(data):
        import json
        import psycopg2
        from psycopg2 import errors
        from psycopg2.errorcodes import UNIQUE_VIOLATION, IN_FAILED_SQL_TRANSACTION, STRING_DATA_RIGHT_TRUNCATION



        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()

        s3 = s3client()
        bucket = s3.Bucket('joblink')
        
        for fname in data:
            download_path = '/tmp/{}'.format(fname)
            bucket.download_file(fname, download_path)
            with open(download_path) as f:
                json_data = json.load(f)
                for link in json_data['job_links']:                        
                    try:
                        cursor.execute(
                            "INSERT INTO joblink (link) VALUES (%s)",
                            (link.lstrip(), ))
                        cursor.execute('commit')
                    except errors.lookup(IN_FAILED_SQL_TRANSACTION):
                        cursor.execute("rollback")
                        cursor.execute(
                            "INSERT INTO joblink (link) VALUES (%s)",
                            (link.lstrip(), ))
                        cursor.execute('commit')
                    except errors.lookup(UNIQUE_VIOLATION):
                        cursor.execute("rollback")
                        continue
                    except errors.lookup(STRING_DATA_RIGHT_TRUNCATION):
                        cursor.execute("rollback")
                        continue
            
        cursor.close()
        dbclient.close()
    

    @task()
    def prepare_bucket_to_summary():
        s3 = s3client()
        s3.create_bucket(Bucket='jobsummary')

    @task()
    def prepare_db_to_summary():
        import psycopg2

        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS summary (
                id SERIAL PRIMARY KEY,
                name TEXT
            )'''
        )
        dbclient.commit()
        cursor.close()
        dbclient.close()

    @task()
    def extract_summary_from_jobsummary():
        import pandas as pd

        s3 = s3client()
        bucket = s3.Bucket('jobsummarychunks')
        
        job_summary = []
        for s3_obj in bucket.objects.all():
            filename = s3_obj.key
            download_path = '/tmp/{}'.format(filename)
            bucket.download_file(filename, download_path)
            df = pd.read_csv(download_path)
            jobsummary = df.get('job_summary')
            summary = jobsummary.to_list()
            job_summary += summary
            break

        data = {'summary': job_summary}
        json_data = json.dumps(data).encode('UTF-8')
        json_fname = 'jobsummary.json'
        s3_Obj = s3.Object('jobsummary', json_fname)
        s3_Obj.put(
            Body=(bytes(json_data))
        )
        return json_fname

    @task()
    def insert_summary_to_db(summary_fname):
        import json
        import psycopg2
        from psycopg2 import errors
        from psycopg2.errorcodes import UNIQUE_VIOLATION, IN_FAILED_SQL_TRANSACTION, STRING_DATA_RIGHT_TRUNCATION



        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()

        s3 = s3client()
        bucket = s3.Bucket('jobsummary')
        download_path = '/tmp/{}'.format(summary_fname)
        bucket.download_file(summary_fname, download_path)
        with open(download_path) as f:
            json_data = json.load(f)
            for summary in json_data['summary']:
                try:
                    cursor.execute(
                        "INSERT INTO summary (name) VALUES (%s)",
                        (summary.lstrip(), ))
                    cursor.execute('commit')
                except errors.lookup(IN_FAILED_SQL_TRANSACTION):
                    cursor.execute("rollback")
                    cursor.execute(
                        "INSERT INTO summary (name) VALUES (%s)",
                        (summary.lstrip(), ))
                    cursor.execute('commit')
                except errors.lookup(STRING_DATA_RIGHT_TRUNCATION):
                    cursor.execute("rollback")
                    continue
        
        cursor.close()
        dbclient.close()
    
    @task()
    def prepare_db_to_company():
        import psycopg2

        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS company (
                id SERIAL PRIMARY KEY,
                name VARCHAR (255) UNIQUE
            )'''
        )
        dbclient.commit()
        cursor.close()
        dbclient.close()
    
    @task()
    def prepare_bucket_to_company():
        s3 = s3client()
        s3.create_bucket(Bucket='jobcompany')
    
    @task()
    def extract_jobcompany():
        import pandas as pd
        import json

        s3 = s3client()
        bucket = s3.Bucket('jobpostingchunks')
        job_companies = []
        for s3_obj in bucket.objects.all():
            filename = s3_obj.key
            download_path = '/tmp/{}'.format(filename)
            bucket.download_file(filename, download_path)
            df = pd.read_csv(download_path)
            company = df.get('company')
            companies = company.to_list()
            job_companies += companies
            break
        
        data = {'company': job_companies}
        json_data = json.dumps(data).encode('UTF-8')
        json_fname = 'jobcompanies.json'
        s3_Obj = s3.Object('jobcompany', json_fname)
        s3_Obj.put(
            Body=(bytes(json_data))
        )
        return json_fname
    
    @task()
    def insert_company_to_db(company_fname):
        import json
        import psycopg2
        from psycopg2 import errors
        from psycopg2.errorcodes import UNIQUE_VIOLATION, IN_FAILED_SQL_TRANSACTION, STRING_DATA_RIGHT_TRUNCATION

        dbclient = psycopg2.connect(
            database='etl_raw_data',
            user='airflow_user',
            password='eserloqpbeq',
            host='10.0.0.20'
            )
        cursor = dbclient.cursor()

        s3 = s3client()
        bucket = s3.Bucket('jobcompany')
        download_path = '/tmp/{}'.format(company_fname)
        bucket.download_file(company_fname, download_path)

        with open(download_path) as f:
            json_data = json.load(f)
            for company in json_data['company']:
                try:
                    cursor.execute(
                        "INSERT INTO company (name) VALUES (%s)",
                        (company.lstrip(), ))
                    cursor.execute('commit')
                except errors.lookup(IN_FAILED_SQL_TRANSACTION):
                    cursor.execute("rollback")
                    cursor.execute(
                        "INSERT INTO company (name) VALUES (%s)",
                        (company.lstrip(), ))
                    cursor.execute('commit')
                except errors.lookup(UNIQUE_VIOLATION):
                        cursor.execute("rollback")
                        continue
                except errors.lookup(STRING_DATA_RIGHT_TRUNCATION):
                    cursor.execute("rollback")
                    continue
        
        cursor.close()
        dbclient.close()
        
        
    
    ## EXAMPLEEEEE process_all(test_start() >> [paralel_1(), paralel_2(), paralel_3()])

    # ITS WORKEEED!!! insert_joblink_to_db(prepare_buckets_to_joblink() >> prepare_db_to_joblink() >> [
    #     extract_joblink_from_jobskill_to_db(),
    #     extract_joblink_from_jobposting_to_db(),
    #     extract_joblink_from_jobsummary_to_db()
    # ])

    # WORKEEEED prepare_bucket_to_summary() >> prepare_db_to_summary() >> insert_summary_to_db(extract_summary_from_jobsummary())
    
    prepare_bucket_to_company() >> prepare_db_to_company() >> insert_company_to_db(extract_jobcompany())

    # extract_jobskills_chunks_to_s3() >> create_table_skills >> transform_jobskills_chunk_to_db() 
    # extract_jobposting_chunks_to_s3()
    # extract_job_summary_chunks_to_s3()
    



    # prepare_db_to_jobskill_transform() >> transform_jobskills_chunk_to_db()
    # prepare_db_to_jobposting_transform() 
    

etl()

