import pandas as pd
import io
import boto3

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

def pd_read_csv_s3(path, *args, **kwargs):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), *args, **kwargs)
