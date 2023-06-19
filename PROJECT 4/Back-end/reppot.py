
import boto3
import json
def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('project_4_table')
    
    response = table.scan()
    items = response['Items']
    
    # Business logic to calculate the balance
    balances = {}
    for item in items:
        account_no = item['accountNo']
        amount = int(item['amount'])
        account_type = item['accountType']
        
        if account_no not in balances:
            balances[account_no] = 0
            
        if account_type == 'D':
            balances[account_no] += amount
        elif account_type == 'C':
            balances[account_no] -= amount
    
    # Prepare the result
    result = []
    for account_no, balance in balances.items():
        # Check if the balance is negative and convert to positive for credit accounts
        if balance < 0:
            balance = abs(balance)
        result.append({
            'accountNo': account_no,
            'balance': balance
        })
    print(result)
    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

        }
    }

    