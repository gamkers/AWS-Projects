import boto3
import json

def lambda_handler(event, context):
    # Extract the account details from the request body
    body = json.loads(event['body'])
    account_no = body['accountNo']
    acc_desc = body['description']
    acc_name = body['accountName']
    acc_type = body['acctType']
    print(account_no,acc_desc,acc_name,acc_type)
    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    
    # Get the DynamoDB table
    table_name = 'Acc_master_g11'  # Replace with your actual table name
    table = dynamodb.Table(table_name)
    
    # Update the account details in DynamoDB
    response = table.update_item(
        Key={'acc_no': account_no},
        UpdateExpression='SET acc_desc = :desc, acc_name = :name, acc_type = :type',
        ExpressionAttributeValues={
            ':desc': acc_desc,
            ':name': acc_name,
            ':type': acc_type
        }
    )
    
    # Prepare the response
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return {
            'statusCode': 200,
            'body': json.dumps('Account details updated successfully'),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            }
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to update account details'),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            }
        }
