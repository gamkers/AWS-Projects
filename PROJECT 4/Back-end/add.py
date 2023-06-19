import json
import boto3

def lambda_handler(event, context):
    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    
    # Get the DynamoDB table
    table_name = 'Acc_master_g11'  # Replace with your actual table name
    table = dynamodb.Table(table_name)
    
    body = json.loads(event['body'])
    account_no = body['accountNo']
    acc_desc = body['description']
    acc_name = body['accountName']
    acc_type = body['acctType']
    
    # Create a dictionary with the variable values
    account_data = {
        'acc_no': account_no,
        'acc_desc': acc_desc,
        'acc_name': acc_name,
        'acc_type': acc_type
    }
    
    # Put the account data into DynamoDB
    table.put_item(Item=account_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps(f"records added for account number: {account_no}"),
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

        }
    }

