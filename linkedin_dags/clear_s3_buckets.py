from airflow.decorators import dag, task
import pendulum

@dag(
    dag_id="clear_s3_buckets",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["services"],
)

def clear_s3():
    @task()
    def clear_all_buckets():
        import boto3

        s3 = boto3.resource('s3', aws_access_key_id='accessKey1', aws_secret_access_key='verySecretKey1', endpoint_url='http://10.0.0.30:8000/')
        for bucket in s3.buckets.all():
            bucket.objects.all().delete()
    
    # @task()
    # def clear_bucket(bucket_name: str):
    #     import boto3

    #     s3 = boto3.resource('s3', aws_access_key_id='accessKey1', aws_secret_access_key='verySecretKey1', endpoint_url='http://10.0.0.30:8000/')
    #     bucket = s3.Bucket(bucket_name)
    #     bucket.objects.all().delete()    

    # clear_bucket('')
clear_s3()
