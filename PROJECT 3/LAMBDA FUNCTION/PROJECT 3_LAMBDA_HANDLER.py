import json
import os
import boto3

dyn=boto3.client("dynamodb")
sqs=boto3.client("sqs")

table_name="group6-transaction_table"

import uuid

def generate_unique_id():
    unique_id = str(uuid.uuid4().int)[:5]
    unique_id = int(unique_id)
    return unique_id

id_int = generate_unique_id()

def generate_txn_id():
    previous = int(os.environ.get("LAST_TXN_ID", id_int))
    current = previous+1
    os.environ["LAST_TXN_ID"]=str(current)
    return current

def add_in_ddb(record, source_system_id):
    items=[]
    for a, i in [("A101", "amt"), ("A104", "gst"), ("A103", "excise_duty")]:
        temp={
            "txn_id":{"N":str(generate_txn_id())},
            "voucher_code":{"S":record["Txn_no"]},
            "txn_type":{"S":"C"},
            "txn_date":{"S":record["tn_dt"]},
            "acc_no":{"S":a},
            "txn_amt":{"S":str(record[i])},
            "source_system_id":{"N":str(source_system_id)},
            "source_system_txn_id":{"S":record["tn_rf_id"]}
        }
        items.append(temp)
    
    for item in items:
        dyn.put_item(
            TableName=table_name,
            Item=item
            )

def lambda_handler(event, context):
    # TODO implement
    
    print("The Message is: ", event["Records"][0]["body"])
    print("The Message has Arrived from Queue: ", event["Records"][0]["eventSourceARN"].split(":")[-1])
    
    record=json.loads(event["Records"][0]["body"])
    print(record)
    
    # Handling for inconsistencies
    
    for key in record.keys():
        if record[key] is None or record[key]=="" or record["amt"] is None or record["amt"]<0:
            sqs.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/445264173157/group-6-p3-dlq",
                MessageBody=json.dumps(record)
                )
            print("Record sent to DLQ")
    
    
    print("It is Queue System Record")
    add_in_ddb(record, 3)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

