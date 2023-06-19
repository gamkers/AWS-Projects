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
    
    # Delete the records based on the account number
    response = table.delete_item(Key={'acc_no': account_no})
    
    return {
        'statusCode': 200,
        'body': json.dumps(f"Deleted records for account number: {account_no}"),
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

        }
    }