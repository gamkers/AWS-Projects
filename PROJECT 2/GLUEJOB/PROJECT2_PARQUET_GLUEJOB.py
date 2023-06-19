import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.window import Window
import uuid
from datetime import datetime
current_date = datetime.now().strftime("%Y-%m-%d")


def generate_unique_id():
    unique_id = str(uuid.uuid4().int)[:5]
    unique_id = int(unique_id)
    return unique_id

args = getResolvedOptions(sys.argv, ["JOB_NAME","file_path","key"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [args["file_path"]],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

df = S3bucket_node1.toDF()

import boto3

# Define the source and destination paths
source_bucket = "groupno6"
source_key = args["key"]
names=source_key.split("/")
destination_bucket = "groupno6"
destination_key = f"aws-project-2/archive/{names[-1]}"

# Create a new S3 client using the default AWS credentials
s3_client = boto3.client("s3")

# Copy the file from the source to the destination
s3_client.copy_object(
    Bucket=destination_bucket,
    Key=destination_key,
    CopySource={
        "Bucket": source_bucket,
        "Key": source_key
    }
)



def generate_unique_id():
    unique_id = str(uuid.uuid4().int)[:5]
    unique_id = int(unique_id)
    return unique_id

# Example usage
id_int = generate_unique_id()

window = Window.orderBy(F.lit(1))

# Transform the DataFrame using selectExpr
result_df = df.selectExpr(
    "code AS voucher_code",
    "'C' AS txn_type",
    "date_format(to_date(tdate, 'dd-MMM-yy'), 'yyyy-MM-dd') AS txn_date",
    "'A101' AS acc_no",
    "trn_amount AS txn_amt",
    "'1' AS source_system_id",
    "trnrefid AS source_system_txn_id"
).union(
    df.selectExpr(
        "code AS voucher_code",
        "'C' AS txn_type",
        "date_format(to_date(tdate, 'dd-MMM-yy'), 'yyyy-MM-dd') AS txn_date",
        "'A102' AS acc_no",
        "vat AS txn_amt",
        "'1' AS source_system_id",
        "trnrefid AS source_system_txn_id"
    )
).union(
    df.selectExpr(
        "code AS voucher_code",
        "'C' AS txn_type",
        "date_format(to_date(tdate, 'dd-MMM-yy'), 'yyyy-MM-dd') AS txn_date",
        "'A103' AS acc_no",
        "excise_duty AS txn_amt",
        "'1' AS source_system_id",
        "trnrefid AS source_system_txn_id"
    )
)


# Add txn_id column using F.concat, F.lit, F.row_number, over, window, and cast
result_df = result_df.withColumn(
    "txn_id",
    F.concat(
        F.lit(id_int),
        F.row_number().over(window)
    ).cast("integer")
)

# Reorder the columns with txn_id as the first column
result_df = result_df.select("txn_id", *result_df.columns[:-1])


failed_df = result_df.filter(F.col("txn_date") > current_date)
result_df = result_df.filter(F.col("txn_date") <= current_date)

failed_dynamic_frame = DynamicFrame.fromDF(failed_df, glueContext, "failed_dynamic_frame")
output_dynamic_frame = DynamicFrame.fromDF(result_df, glueContext, "output_dynamic_frame")


# Script generated for node S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.region": "us-east-1",
        "dynamodb.output.tableName": "group6-transaction_table",
    }
)

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=failed_dynamic_frame,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://groupno6/aws-project-2/failed-records/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

# Delete the file from the source folder
s3_client.delete_object(
    Bucket=source_bucket,
    Key=source_key
)

job.commit()