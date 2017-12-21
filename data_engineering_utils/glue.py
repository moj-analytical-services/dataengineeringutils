import pandas as pd 
import numpy as np
import boto3
from io import StringIO
glue_client = boto3.client('glue', 'eu-east-1')
s3_resource = boto3.resource('s3')

# Super basic atm assumes everything that isn't an int or double as a string
def get_db_col_types_from_df(df) :
    col_names = list(df)
    table_col_meta = []
    for col in col_names :
        if df[col].dtype == np.int64 :
            table_col_meta.append({'Name' : col, 'Type' : 'bigint'})
        elif df[col].dtype == np.float :
            table_col_meta.append({'Name' : col, 'Type' : 'double'})
        else :
            table_col_meta.append({'Name' : col, 'Type' : 'string'})
    
    return(table_col_meta)

# If overides is not supplied assumes all columns are strings
def get_table_col_meta_template(colnames, overrides = None) :
    if overrides is None :
        overrides = []
    
    base_types = get_base_data_types()
    
    keys = list(overrides)

    # Error checking
    for k in keys :
        if overrides[k] not in base_types :
            raise ValueError("Column name: \"{}\" in input overrides: has an invalid datatype. Valid datatypes are {}".format(k,", ".join(base_types)))
    
    # Create meta data
    col_meta = []
    for c in colnames :
        col_meta.append({'Name' : c,
                         'Type' : 'string' if c not in keys else overrides[c]})
    return(col_meta)

# Get a list of accepted base datatypes for glue table definitions
def get_base_data_types() :
    return base_types = ['boolean', 'bigint', 'double', 'string', 'timestamp', 'date']

# Save a dataframe to an S3 bucket (removes headers)
def df_to_csv_s3(df, bucket, path, header=False, index=False):
    csv_buffer = StringIO()
    
    #Skip headers is necessary for now - see here: https://twitter.com/esh/status/811396849756041217
    df.to_csv(csv_buffer, index=False, header=False)
    
    s3_f = s3_resource.Object(bucket, path)
    response = s3_f.put(Body=csv_buffer.getvalue())

# Returns a table schema definition for csv or orc
def get_table_definition_template(table_name = '', table_desc = '', table_col_meta = [], location = '', template_type = 'csv') :
    if template_type == 'csv' :
        table_def = {
            'Description': table_desc,
            'Name': table_name,
            'PartitionKeys': [],
            'Owner': "hadoop",
            'Retention': 0,
            'Parameters': {
                'EXTERNAL': 'TRUE',
                'classification': 'csv',
                'delimiter': ',',
                'compressionType': 'none'
            },
            'StorageDescriptor': {
                'BucketColumns': [],
                'Columns': table_col_meta,
                 'Compressed': False,
                 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                 'Location': location,
                 'NumberOfBuckets': -1,
                 'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                 'Parameters': {},
                 'SerdeInfo': {
                     'Parameters': {
                         'field.delim': ',',
                         'serialization.format': ','
                     },
                     'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                 },
                 'SkewedInfo': {
                     'SkewedColumnNames': [],
                     'SkewedColumnValueLocationMaps': {},
                     'SkewedColumnValues': []
                 },
                 'SortColumns': [],
                 'StoredAsSubDirectories': False
            },
            'TableType': 'EXTERNAL_TABLE'
        }
    
    elif template_type == 'orc':
        table_def = {
            'Name': table_name,
            'Owner': 'owner',
            'PartitionKeys': [],
            'Retention': 0,
            'StorageDescriptor': {
                'BucketColumns': [],
                'Columns': table_col_meta,
                'Compressed': False,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat',
                'NumberOfBuckets': -1,
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat',
                'SerdeInfo': {
                    'Parameters': {},
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
                },
                'SortColumns': [],
                'StoredAsSubDirectories': False
            },
            'TableType': 'EXTERNAL_TABLE',
        }
    else :
        table_def = None
        
    return(table_def)

# Add table to database in glue
def create_table_from_def(dbname, tablename, spec):
    try:
        glue_client.delete_table(
            DatabaseName=dbname,
            Name=tablename
        )
    except:
        pass

    response = glue_client.create_table(
        DatabaseName=dbname,
        TableInput=spec)

# Does what it says on the tin
def take_script_and_run_job(input_script_path, output_script_path, role, job_name, script_bucket = "alpha-dag-data-engineers-raw", tempdir = "s3://alpha-dag-data-engineers-raw/athena_out"):

    s3_f = s3_resource.Object(script_bucket, output_script_path)
    response= s3_f.put(Body=open(input_script_path, "rb"))

    job = {'AllocatedCapacity': 2,
     'Command': {
      'Name': 'glueetl',
      'ScriptLocation': 's3://{}/{}'.format(script_bucket, output_script_path)
     },
     'DefaultArguments': {'--TempDir': tempdir,
      '--job-bookmark-option': 'job-bookmark-disable'},
     'ExecutionProperty': {'MaxConcurrentRuns': 1},
     'MaxRetries': 0,
     'Name': job_name,
     'Role': role}
    
    response = glue_client.create_job(**job)
    response = glue_client.start_job_run(JobName=job_name)


