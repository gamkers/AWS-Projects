import random
import string
import json
from datetime import datetime, date, timedelta
from random import randrange
import boto3
# import app_config as c

def lambda_handler(event, context):
    
    sqs_url="https://sqs.us-east-1.amazonaws.com/445264173157/group-6-p3-queue"
    msg=get_transaction_message()
    msgstr=json.dumps(msg)
    send_message_to_q(message=msgstr,queue_url=sqs_url)
    
    
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }
    
    
 

#--------------------------------------------------------------------------------


def get_transaction_message():
    txn_amt= round(random.uniform(50, 4599), 2)
    msg={}
    msg['tn_rf_id'] = tn_rf_id()
    msg['Txn_no'] = Txn_no()
    msg['tn_dt'] = tn_dt()
    msg['amt'] = txn_amt
    msg['gst'] = round(txn_amt*0.05 ,2)
    msg['excise_duty'] = round(txn_amt*0.01,2)    
    #print(msg)
    return msg
    
def tn_rf_id():
    generated_string = ''.join(random.choices(string.ascii_letters + string.digits, k=14))
    return generated_string 
    
def Txn_no():    
    numbers = random.sample(string.digits, 10)
    return ''.join(numbers)
    
def tn_dt():
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    d1 = datetime.strptime('1/1/2000 00:01', '%m/%d/%Y %H:%M')
    d2 = datetime.strptime('12/31/2009 23:59', '%m/%d/%Y %H:%M')
    delta = d2 - d1
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    newdate=d1 + timedelta(seconds=random_second)    
    return newdate.strftime('%Y-%m-%d %H:%M %p')
#--------------------------------------------------------------------------------

def send_message_to_q(message,queue_url):
    # Create SQS client
    #sqs_client = boto3.client('sqs')
    sqs_client = boto3.client('sqs',
                             aws_access_key_id='AKIAWPK6QDBS4626DDXB',
                             aws_secret_access_key='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',
                             region_name='us-east-1'
                             )
    
    # Send message to SQS queue
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        DelaySeconds=10,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'Transaction Record'
            },
            'Format': {
                'DataType': 'String',
                'StringValue': 'JSON'
            }
        },
        MessageBody=(
            message
        )
    )
#--------------------------------------------------------------------------------

lambda_handler('','')
