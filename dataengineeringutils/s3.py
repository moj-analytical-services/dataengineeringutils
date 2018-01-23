import pandas as pd
import io
import boto3
import re
import os

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

def s3_path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    return bucket, key

def pd_read_csv_s3(path, *args, **kwargs):
    bucket, key = s3_path_to_bucket_key(path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), *args, **kwargs)

def pd_write_csv_s3(df, path, *args, **kwargs):
    bucket, key = s3_path_to_bucket_key(path)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, *args, **kwargs)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

def upload_file_to_s3_from_path(input_path, bucket_name, output_path):
   s3_client.upload_file(input_path, bucket_name, output_path)
   return "s3://{}/{}".format(bucket_name, output_path)

def upload_meta_data_folder_to_s3(meta_data_base_folder, bucket) :
    """
    Uploads the same meta_data/ folder structure to it's S3 bucket
    """
    meta_listing = os.listdir(meta_data_base_folder)
    regex = ".+(\.json)$"
    meta_listing = [f for f in meta_listing if re.match(regex, f)]
    for m in meta_listing:
        meta_local_path = os.path.join(meta_data_base_folder, m)
        path = upload_file_to_s3_from_path(meta_local_path, bucket, meta_local_path)

def delete_file_from_s3(bucket_name, key):
    s3_resource.Object(bucket_name, key).delete()


def delete_folder_from_bucket(bucket, folder):

    if '/' in bucket:
        raise ValueError("You provided bucket name {}, but this has disallowed punctuation in it".format(bucket))

    if folder[-1:] != "/":
        message = """The folder path you provided doesn't end with a /.
            Stopping, because you could accidentally end up deleting more than you expected.
            See https://stackoverflow.com/a/11427712/1779128"""
        raise ValueError(message)

    bucket = s3_resource.Bucket(bucket)

    bucket.objects.filter(Prefix=folder).delete()

def first_n_bytes_of_s3_object_to_lines(s3_path, num_bytes=1024, encoding="utf-8"):
    """
    Read the first n bytes of an s3 object and return a list of lines
    Args:
        s3_path: The full path to the s3 object
        num_bytes: The number of bytes of the file to read
        encoding: The character encoding to use to convert these bytes to a string
    Returns:
        lines: A list of strings, each element representing a line
    """

    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()['Body'].read(num_bytes).decode(encoding)
    lines = text.splitlines()
    return lines