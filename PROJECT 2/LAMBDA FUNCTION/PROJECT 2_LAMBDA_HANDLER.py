import json
import urllib.parse
import boto3

def start_glue_job(job_name, file_uri, object_key):
    glue_client = boto3.client('glue')
    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments={
            "--file_path": file_uri,
            "--key": object_key
        }
    )
    print(f"Started Glue job: {job_name}, JobRunId: {response['JobRunId']}")

def lambda_handler(event, context):
    # Extract bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    # Construct the file URI
    file_uri = f"s3://{bucket_name}/{object_key}"
    
    # Print the file URI
    print(file_uri)
    
    # Check if file URI contains 'parquet'
    if 'parquet' in file_uri:
        start_glue_job('group6 parq', file_uri, object_key)
    # Check if file URI contains 'avro'
    elif 'avro' in file_uri:
        start_glue_job('group6 avro', file_uri, object_key)
    else:
        print("Unsupported file type.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }