import pandas as pd
import io
import boto3

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

def path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    return bucket, key

def pd_read_csv_s3(path, *args, **kwargs):
    bucket, key = path_to_bucket_key(path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), *args, **kwargs)

def pd_write_csv_s3(df, path, *args, **kwargs):
    bucket, key = path_to_bucket_key(path)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, *args, **kwargs)
    s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())