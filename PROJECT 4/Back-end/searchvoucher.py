import json
import boto3
from decimal import Decimal

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def lambda_handler(event, context):
    # Parse the request body
    body = json.loads(event['body'])
    voucher_no = body['voucherNo']

    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('group-11-transaction_table')  # Replace with your DynamoDB table name
    print(voucher_no)
    # Perform the search query
    response = table.scan(
        FilterExpression='voucher_code = :voucher',
        ExpressionAttributeValues={
            ':voucher': voucher_no
        }
    )

    # Retrieve the matching vouchers
    vouchers = response['Items']
    
    # Convert Decimal objects to float
    vouchers = convert_decimals(vouchers)

    # Prepare the response
    response_body = json.dumps(vouchers)
    print(response_body)

    return {
        'statusCode': 200,
        'body': response_body,
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

        }
    }
