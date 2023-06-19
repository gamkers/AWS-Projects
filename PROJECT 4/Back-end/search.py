import boto3
import json
def lambda_handler(event, context):
    # print(event)
    # Extract the account number from the request
    body = json.loads(event['body'])
    account_no = body['accountNo']
    print(account_no)
    # account_no = event['body']['accountNo']

    
    # Retrieve the account description from DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Acc_master_g11')  # Replace with your DynamoDB table name
    response = table.get_item(Key={'acc_no': account_no})
    # item = response.get('Item')
    # print(item["acc_desc"])
    
    # Check if the account exists in DynamoDB
    if 'Item' in response:
        account_description = response['Item']['acc_desc']
        account_no = response['Item']['acc_no']
        acc_name = response['Item']['acc_name']
        acc_type = response['Item']['acc_type']
        print(account_description)
    else:
        return {
            'statusCode': 404,
            'body': json.dumps('Account not found'),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            }
        }

    # Prepare the response
    jsonStr = json.dumps({
        'description': account_description,
        'acc_no': account_no,
        'acc_name': acc_name,
        'acc_type': acc_type
    
    })
    
    return {
        'statusCode': 200,
        'body': jsonStr,
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

        }
    }
