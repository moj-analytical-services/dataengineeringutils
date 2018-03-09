import pandas as pd
import os
import re
import numpy as np
import boto3
import json
import pkg_resources
import time
from urllib.request import urlretrieve
import dataengineeringutils.meta as meta_utils
from dataengineeringutils.datatypes import translate_metadata_type_to_type
from dataengineeringutils.utils import dict_merge, read_json, _end_with_backslack
from dataengineeringutils.s3 import s3_path_to_bucket_key, upload_file_to_s3_from_path, delete_folder_from_bucket, get_file_list_from_bucket

from io import StringIO
glue_client = boto3.client('glue', 'eu-west-1')
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

import logging
log = logging.getLogger(__name__)

def df_to_csv_s3(df, bucket, path, index=False, header=False):
    """
    Takes a pandas dataframe and writes out to s3
    """
    csv_buffer = StringIO()

    #Skip headers is necessary for now - see here: https://twitter.com/esh/status/811396849756041217
    df.to_csv(csv_buffer, index=index, header=header)

    s3_f = s3_resource.Object(bucket, path)
    response = s3_f.put(Body=csv_buffer.getvalue())


def get_table_definition_template(template_type = 'csv', **kwargs):
    """Get a definition template that can be used with Glue for the various template types

    Args:
        template_type: Allowed values are {'csv', 'parquet', 'avro', 'orc'}
        **kwargs: Arbitrary keyword arguments which will be added to the template
    """
    base_io = pkg_resources.resource_stream(__name__, "specs/base.json")
    base = json.load(base_io)

    conversion = {
        "avro": pkg_resources.resource_stream(__name__, "specs/avro_specific.json"),
        "csv": pkg_resources.resource_stream(__name__, "specs/csv_specific.json"),
        "csv_quoted_nodate": pkg_resources.resource_stream(__name__, "specs/csv_quoted_nodate_specific.json"),
        "regex": pkg_resources.resource_stream(__name__, "specs/regex_specific.json"),
        "orc": pkg_resources.resource_stream(__name__, "specs/orc_specific.json"),
        "par": pkg_resources.resource_stream(__name__, "specs/par_specific.json"),
        "parquet": pkg_resources.resource_stream(__name__, "specs/par_specific.json")
        }

    specific_io = conversion[template_type]
    specific = json.load(specific_io)
    dict_merge(base, specific)
    dict_merge(base, kwargs)
    return base


def overwrite_or_create_database(db_name, db_description=""):
    """
    Creates a database in Glue.  If it exists, delete it
    """
    db = {
        "DatabaseInput": { 
            "Description": db_description,
            "Name": db_name,
        }
    }

    try:
        glue_client.delete_database(Name=db_name)
        log.debug("Deleting database: {}".format(db_name))
    except :
        pass

    log.debug("Creating database: {}".format(db_name))
    glue_client.create_database(**db)

# Add table to database in glue
def create_table_in_glue_from_def(db_name, table_name, table_spec) :
    try :
        glue_client.delete_table(
            DatabaseName=db_name,
            Name=table_name
        )
    except :
        pass

    response = glue_client.create_table(
        DatabaseName=db_name,
        TableInput=table_spec)


def create_glue_job_definition(**kwargs):

    template_io = pkg_resources.resource_stream(__name__, "specs/glue_job.json")
    template = json.load(template_io)

    if 'Name' in kwargs:
        template["Name"] = kwargs['Name']
    else :
        raise ValueError("You must give your job a name, using the Name kwarg")

    if 'Role' in kwargs:
        template["Role"] = kwargs["Role"]
    else :
        raise ValueError("You must give your job a role, using the Role kwarg")

    if 'ScriptLocation' in kwargs:
        template["Command"]["ScriptLocation"] = kwargs["ScriptLocation"]
    else :
        raise ValueError("You must assign a ScriptLocation to your job, using the ScriptLocation kwarg")

    if 'TempDir' in kwargs:
        template["DefaultArguments"]["--TempDir"] = kwargs["TempDir"]
    else :
        raise ValueError("You must give your job a temporary directory to work in, using the TempDir kwarg")

    if 'extra-files' in kwargs:
        template["DefaultArguments"]["--extra-files"] = kwargs["extra-files"]
    else :
        template["DefaultArguments"].pop("--extra-files", None)

    if 'extra-py-files' in kwargs:
        template["DefaultArguments"]["--extra-py-files"] = kwargs["extra-py-files"]
    else :
        template["DefaultArguments"].pop("--extra-py-files", None)

    if 'MaxConcurrentRuns' in kwargs:
        template["ExecututionProperty"]["MaxConcurrentRuns"] = kwargs["MaxConcurrentRuns"]

    if 'MaxRetries' in kwargs:
        template["MaxRetries"] = kwargs["MaxRetries"]

    if 'AllocatedCapacity' in kwargs:
        template["AllocatedCapacity"] = kwargs["AllocatedCapacity"]
    else:
        template["AllocatedCapacity"] = 2


    return template


# Does what it says on the tin
def take_script_and_run_job(input_script_path, output_script_path, role, job_name, script_bucket = "alpha-dag-data-engineers-raw", temp_dir = "s3://alpha-dag-data-engineers-raw/athena_out"):
    """
    See https://github.com/awsdocs/aws-glue-developer-guide/blob/1d6cb6174ee1f182c7da7e44f4071c6f10dfbe63/doc_source/aws-glue-programming-python-glue-arguments.md
    """
    s3_f = s3_resource.Object(script_bucket, output_script_path)
    response= s3_f.put(Body=open(input_script_path, "rb"))

    job = {'AllocatedCapacity': 2,
     'Command': {
      'Name': 'glueetl',
      'ScriptLocation': 's3://{}/{}'.format(script_bucket, output_script_path)
     },
     'DefaultArguments': {'--TempDir': temp_dir,
      '--job-bookmark-option': 'job-bookmark-disable'},
     'ExecutionProperty': {'MaxConcurrentRuns': 1},
     'MaxRetries': 0,
     'Name': job_name,
     'Role': role}

    response = glue_client.create_job(**job)
    response = glue_client.start_job_run(JobName=job_name)

def get_glue_column_spec_from_metadata(metadata):
    """
    Use metadata to create a column spec which will fit into the Glue table template
    """
    columns = metadata["columns"]
    glue_columns = []
    for c in columns:
        new_c = {}
        new_c["Name"] = c["name"]
        new_c["Comment"] = c["description"]
        new_c["Type"] = translate_metadata_type_to_type(c["type"], "glue")
        glue_columns.append(new_c)
    return glue_columns


def metadata_to_glue_table_definition(tbl_metadata, db_metadata):
    """
    Use metadata in json format to create a table definition
    """

    database_location = db_metadata["location"]
    table_location_relative = tbl_metadata["location"]
    table_location_absolute = os.path.join(database_location, table_location_relative)

    template_type = tbl_metadata["data_format"]
    table_definition = get_table_definition_template(template_type)
    column_spec = get_glue_column_spec_from_metadata(tbl_metadata)

    table_definition["Name"] = tbl_metadata["table_name"]
    table_definition["Description"] = tbl_metadata["table_desc"]

    table_definition['StorageDescriptor']['Columns'] = column_spec
    table_definition['StorageDescriptor']["Location"] = table_location_absolute

    if "glue_specific" in tbl_metadata:
        dict_merge(table_definition, tbl_metadata["glue_specific"])

        # If there are partition keys, remove them from table
        if "PartitionKeys" in tbl_metadata["glue_specific"]:
            pks = tbl_metadata["glue_specific"]["PartitionKeys"]
            pk_names = [pk["Name"] for pk in pks]
            cols = table_definition["StorageDescriptor"]["Columns"]
            cols = [col for col in cols if col["Name"] not in pk_names]
            table_definition["StorageDescriptor"]["Columns"] = cols

    return table_definition

def populate_glue_catalogue_from_metadata(table_metadata, db_metadata, check_existence = True):
    """
    Take metadata and make requisite calls to AWS API using boto3
    """

    database_name = db_metadata["name"]
    database_description = ["description"]

    table_name = table_metadata["table_name"]

    tbl_def = metadata_to_glue_table_definition(table_metadata, db_metadata)

    if check_existence:
        try:
            glue_client.get_database(Name=database_name)
        except glue_client.exceptions.EntityNotFoundException:
            overwrite_or_create_database(database_name, db_metadata["description"])

        try:
            glue_client.delete_table(DatabaseName=database_name, Name=table_name)
        except glue_client.exceptions.EntityNotFoundException:
            pass

    return glue_client.create_table(
        DatabaseName=database_name,
        TableInput=tbl_def)


def metadata_folder_to_database(folder_path, delete_db = True, db_suffix = None):
    """
    Take a metadata folder and build the database and all tables
    Args:
        delete_db bool: Delete the database before starting
        db_suffix: If provided, metadata will be modified so that the database name, and s3 data locations include the folder suffix
    """
    files = os.listdir(folder_path)
    files = set([f for f in files if re.match(".+\.json$", f)])

    if "database.json" in files:
        db_metadata = read_json(os.path.join(folder_path, "database.json"))

        if db_suffix:
            str_to_add = "_" + db_suffix
            if db_metadata["location"][-1] == "/":
                db_metadata["location"] = db_metadata["location"][:-1]
            db_metadata["location"] = db_metadata["location"] + str_to_add
            db_metadata["name"] = db_metadata["name"] + str_to_add

        database_name = db_metadata["name"]

        try:
            glue_client.delete_database(Name=database_name)
        except glue_client.exceptions.EntityNotFoundException:
            pass
        overwrite_or_create_database(database_name, db_metadata["description"])

    else:
        raise ValueError("database.json not found in metadata folder")
        return None

    table_paths = files.difference({"database.json"})
    for table_path in table_paths:
        table_path = os.path.join(folder_path, table_path)
        table_metadata = read_json(table_path)
        populate_glue_catalogue_from_metadata(table_metadata, db_metadata, check_existence=False)


def glue_job_dir_to_s3(local_glue_jobs_dir, s3_glue_jobs_dir, include_folders = None, exclude_folders = None) :
    """
    Iterate though all folders in the glue_job dir and upload them to a corresponsing 
    glue_job dir in s3. Each folder in local_glue_jobs_dir is uploaded using glue_job_folder_to_s3.
    Provide list of folder glue_job folder names in include_folders and exclude_folders to include and exclude them from the upload. 
    """
    local_glue_jobs_dir = _end_with_backslack(local_glue_jobs_dir)

    # Do checks
    if not (include_folders is None or isinstance(include_folders, list)) :
        raise ValueError('include_folders must be a list or None')
    if not (exclude_folders is None or isinstance(exclude_folders, list)) : 
        raise ValueError('exclude_folders must be a list or None')

    if include_folders is not None and exclude_folders is not None :
        if len(set(include_folders).intersection(set(exclude_folders))) != 0 :
            raise ValueError('Some folders are listed in both include_folders and exclude_folders')

    # Create list of folders
    glue_job_folders = [d for d in os.listdir(local_glue_jobs_dir) if os.path.isdir(os.path.join(local_glue_jobs_dir, d)) and d[0] != '.']
    if include_folders is not None :
        test_include = [i in glue_job_folders for i in include_folders]
        if not all(test_include) :
            raise ValueError('One of the folders listed in include_folders does not exist in {}'.format(local_glue_jobs_dir))
        else :
            glue_job_folders = include_folders
    if exclude_folders is not None :
        glue_job_folders = [g for g in glue_job_folders if g not in exclude_folders]

    s3_glue_jobs_dir = _end_with_backslack(s3_glue_jobs_dir)

    for glue_job in glue_job_folders :
        glue_job_folder_to_s3(local_glue_jobs_dir + glue_job + '/', s3_glue_jobs_dir)

def glue_job_folder_to_s3(local_base, s3_glue_jobs_dir):
    """
    Take a folder structure on local disk and transfer to s3.

    Folder must be formatted as follows:
    base dir
      job.py
      glue_py_resources/
        zip and python files
        zip_urls <- file containing urls of additional zip files e.g. on github
      glue_resources/
        txt, sql, json, or csv files

    The folder name base dir will be in the folder s3_path_to_glue_jobs_folder
    """
    local_base = _end_with_backslack(local_base)
    s3_glue_jobs_dir = _end_with_backslack(s3_glue_jobs_dir)

    base_dir_listing = os.listdir(local_base)

    if s3_glue_jobs_dir[-1:] != "/":
        raise ValueError("s3_glue_jobs_dir must be a folder and therefore must end in a /")

    s3_job_folder_path = ''.join([s3_glue_jobs_dir] + local_base.split('/')[-2:])
    
    # Upload job
    bucket, bucket_folder = s3_path_to_bucket_key(s3_job_folder_path)
    
    # Check that there is at least a job.py in the given folder and then upload job if appropriate
    if 'job.py' not in base_dir_listing :
        if local_base.split('/')[-2] != 'shared_job_resources' :
            raise ValueError("Could not find job.py in base directory provided ({}), stopping.\nOnly folder allowed to have no job.py is a folder named shared_job_resources".format(local_base))
    else :
        local_job_path = os.path.join(local_base, "job.py")
        job_path = upload_file_to_s3_from_path(local_job_path, bucket, "{}/job.py".format(bucket_folder))

    # Upload all the .py or .zip files in resources
    # Check existence of folder, otherwise skip
    resources_path = os.path.join(local_base, "glue_resources")
    if os.path.isdir(resources_path):
        resource_listing = os.listdir(os.path.join(local_base, 'glue_resources'))
        regex = ".+(\.sql|\.json|\.csv|\.txt)$"
        resource_listing = [f for f in resource_listing if re.match(regex, f)]

        for f in resource_listing:
            resource_local_path = os.path.join(local_base, "glue_resources", f)
            path = upload_file_to_s3_from_path(resource_local_path, bucket, "{}/glue_resources/{}".format(bucket_folder,f))


    # Upload all the .py or .zip files in resources
    # Check existence of folder, otherwise skip
    py_resources_path = os.path.join(local_base, "glue_py_resources")
    delete_these_paths = []
    if os.path.isdir(py_resources_path):

        zip_urls_path = os.path.join(py_resources_path, "github_zip_urls.txt")
        if os.path.exists(zip_urls_path):

            with open(zip_urls_path, "r") as f:
                urls = f.readlines()

            urls = [url for url in urls if len(url) > 10]

            for i, url in enumerate(urls):

                this_zip_path = os.path.join(py_resources_path,"{}.zip".format(i))
                urlretrieve(url,this_zip_path)
                new_zip_path = unnest_github_zipfile_and_return_new_zip_path(this_zip_path)
                os.remove(this_zip_path)
                delete_these_paths.append(new_zip_path)


        resource_listing = os.listdir(os.path.join(local_base, 'glue_py_resources'))
        regex = ".+(\.py|\.zip)$"
        resource_listing = [f for f in resource_listing if re.match(regex, f)]

        for f in resource_listing:
            resource_local_path = os.path.join(local_base, "glue_py_resources", f)
            path = upload_file_to_s3_from_path(resource_local_path, bucket, "{}/glue_py_resources/{}".format(bucket_folder,f))

        # Remember to delete the files we downloaded
        for this_path in delete_these_paths:
            os.remove(this_path)
    
def get_glue_job_and_resources_from_s3(s3_base_path) :
    
    s3_base_path = _end_with_backslack(s3_base_path)
    
    bucket, bucket_folder = s3_path_to_bucket_key(s3_base_path)
    bucket_folder = bucket_folder[:-1]
    
    shared_bucket_folder = '/'.join(bucket_folder.split('/')[:-1]) + '/shared_job_resources'
    
    files_list = get_file_list_from_bucket(bucket, bucket_folder)
    
    if "{}/job.py".format(bucket_folder) not in files_list:
        raise ValueError("Cannot find job.py in the folder specified ({}), stopping".format(bucket_folder))
    else:
        job_path = "s3://{}/{}/job.py".format(bucket, bucket_folder)

    try : 
        shared_files_list = get_file_list_from_bucket(bucket, shared_bucket_folder)
    except :
        shared_files_list = []
    
    # Do py_resources
    py_resources = [f for f in files_list if "/glue_py_resources/" in f]
    py_shared_resources = [f for f in shared_files_list if "/glue_py_resources/" in f]
    
    py_resources = py_resources + py_shared_resources
    py_resources = ["s3://{}/{}".format(bucket, f) for f in py_resources]
    py_resources = ",".join(py_resources)
    
    # Do resources
    resources = [f for f in files_list if "/glue_resources/" in f]
    shared_resources = [f for f in shared_files_list if "/glue_resources/" in f]
    
    resources = resources + shared_resources
    resources = ["s3://{}/{}".format(bucket, f) for f in resources]
    resources = ",".join(resources)
    
    if " " in resources or " " in py_resources :
        raise ValueError("The files in glue_resources and glue_py_resources must not have spaces in their filenames")
    
    return (job_path, resources, py_resources)

def glue_folder_in_s3_to_job_spec(s3_base_path, **kwargs) :
    """
    Given a set of files uploaded to s3 in a specific format, use them to create a glue job
    """

    #Base path should be a folder.  Ensure ends in "/"
    # Otherwise listing the bucket could cause problems in e.g. the case there are two jobs, job_1 and job_12

    (job_path, resources, py_resources) = get_glue_job_and_resources_from_s3(s3_base_path)

    kwargs["ScriptLocation"] = job_path
    if resources != '':
        kwargs["extra-files"] = resources
    if py_resources != '':
        kwargs["extra-py-files"] = py_resources
    kwargs["TempDir"] = "s3://{}/{}/{}/temp_dir/".format(bucket, bucket_folder, kwargs["Name"])

    job_spec = create_glue_job_definition(**kwargs)

    return job_spec

def get_list_of_files_from_s3_text_file(s3_path_to_text_file) :
    bytes_io = s3.s3_path_to_bytes_io(s3_path_to_text_file)
    resources = []
    for line in bytes_io.readlines():
        resources.append(line.decode("utf-8"))

    return resources

def delete_all_target_data_from_database(database_metadata_path):
    files = os.listdir(database_metadata_path)
    files = set([f for f in files if re.match(".+\.json$", f)])

    if "database.json" in files:
        db_metadata = read_json(os.path.join(database_metadata_path, "database.json"))
        database_name = db_metadata["name"]
    else:
        raise ValueError("database.json not found in metadata folder")
        return None

    table_paths = files.difference({"database.json"})
    for table_path in table_paths:
        table_path = os.path.join(database_metadata_path, table_path)
        table_metadata = read_json(table_path)
        location = table_metadata["location"]
        bucket, bucket_folder = s3_path_to_bucket_key(location)
        delete_folder_from_bucket(bucket, bucket_folder)

def run_glue_job_as_airflow_task(s3_glue_job_folder, name, role, job_args, delete_job_when_done = True, init_wait_time = 2, interval_wait_time = 1, allocated_capacity = None, max_retries = None, max_concurrent_runs = None, **kwargs) :

    start_job_response, job_spec = run_glue_job_from_s3_folder_template(s3_glue_job_folder, name, role, job_args = None, allocated_capacity = None, max_retries = None, max_concurrent_runs = None)

    # Let AWS spin up spark session (normally 2 mins if warmed up)
    time.sleep(init_wait_time*60)

    job_running = True
    while job_running :
        job_status = glue_client.get_job_run(JobName = name, RunId = start_job_response['JobRunId'])
        if job_status['JobRun']['JobRunState'].lower() == 'running' :
            time.sleep(interval_wait_time*60)
        elif job_status['JobRun']['JobRunState'].lower() == 'succeeded' :
            job_running = False
        else :
            raise ValueError('Something bad happened.\nJob state was: {} (Note job not deleted).\n***OUTPUTING JOB ERROR***\n{}'.format(job_status['JobRun']['JobRunState'].lower(), job_status['JobRun']['ErrorMessage']))

    if delete_job_when_done :
        cleanup_response = glue_client.delete_job(JobName = name)

    return start_job_response['JobRunId']

def run_glue_job_from_s3_folder_template(s3_glue_job_folder, name, role, job_args = None, allocated_capacity = None, max_retries = None, max_concurrent_runs = None) :
    
    s3_glue_job_folder = _end_with_backslack(s3_glue_job_folder)
    
    job_def_kwargs = {}
    job_def_kwargs['Name'] = name
    job_def_kwargs['Role'] = role
    if allocated_capacity is not None :
        job_def_kwargs['AllocatedCapacity'] = allocated_capacity
    if max_retries is not None :
        job_def_kwargs['MaxRetries'] = max_retries
    if max_concurrent_runs is not None :
        job_def_kwargs['MaxConcurrentRuns'] = max_concurrent_runs

    bucket, bucket_folder = s3_path_to_bucket_key(s3_glue_job_folder)
    
    job_spec = glue_folder_in_s3_to_job_spec(s3_glue_job_folder, **job_def_kwargs)

    del_response = delete_job(name)
    response = glue_client.create_job(**job_spec)

    if job_args:
        response = glue_client.start_job_run(JobName=name, Arguments = job_args)
    else:
       response = glue_client.start_job_run(JobName=name)
    return response, job_spec

def run_glue_job_from_local_folder_template(local_base, s3_glue_jobs_dir, name, role, job_args = None, allocated_capacity = None, max_retries = None, max_concurrent_runs = None):
    """
    Take a local folder layed out using our agreed folder spec, upload to s3, and run

    job_args is a dictionary that is passed to the glue job when it is run on aws.
    """

    local_base = _end_with_backslack(local_base)
    s3_glue_jobs_dir = _end_with_backslack(s3_glue_jobs_dir)

    s3_base_path = ''.join([s3_glue_jobs_dir] + local_base.split('/')[-2:])

    # Create kwargs for job defintion these will be used in glue_create_job_defintion
    job_def_kwargs = {}
    job_def_kwargs['Name'] = name
    job_def_kwargs['Role'] = role
    if allocated_capacity is not None :
        job_def_kwargs['AllocatedCapacity'] = allocated_capacity
    if max_retries is not None :
        job_def_kwargs['MaxRetries'] = max_retries
    if max_concurrent_runs is not None :
        job_def_kwargs['MaxConcurrentRuns'] = max_concurrent_runs

    bucket, bucket_folder = s3_path_to_bucket_key(s3_base_path)
    bucket_folder = bucket_folder + '/'
    delete_folder_from_bucket(bucket, bucket_folder)

    glue_job_folder_to_s3(local_base, s3_base_path)

    job_spec = glue_folder_in_s3_to_job_spec(s3_base_path, **job_def_kwargs)

    response = glue_client.create_job(**job_spec)
    if job_args:
        response = glue_client.start_job_run(JobName=name, Arguments = job_args)
    else:
       response = glue_client.start_job_run(JobName=name)
    return response, job_spec

def delete_job(job_name):
    try:
        return glue_client.delete_job(JobName=job_name)
    except:
        return "No job with that name found"

import tempfile
import zipfile
import shutil
def unnest_github_zipfile_and_return_new_zip_path(zip_path):
    """
    When we download a zipball from github like this one:
    https://github.com/moj-analytical-services/gluejobutils/archive/master.zip

    The python lib is nested in the directory like:
    gluejobutils-master/gluejobutils/py files

    The glue docs say that it will only work without this nesting:
    docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html

    This function creates a new, unnested zip file, and returns the path to it

    """

    original_file_name = os.path.basename(zip_path)
    original_dir = os.path.dirname(zip_path)
    new_file_name = original_file_name.replace(".zip", "_new")

    with tempfile.TemporaryDirectory() as td:
        myzip = zipfile.ZipFile(zip_path, 'r')
        myzip.extractall(td)
        nested_folder_to_unnest = os.listdir(td)[0]
        nested_path = os.path.join(td, nested_folder_to_unnest)
        output_path = os.path.join(original_dir, new_file_name)
        final_output_path = shutil.make_archive(output_path, 'zip', nested_path)

    return final_output_path
