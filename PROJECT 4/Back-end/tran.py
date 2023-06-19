
import json
import uuid
import boto3

def generate_unique_id():
    unique_id = str(uuid.uuid4().int)[:5]
    unique_id = int(unique_id)
    return unique_id

# Example usage


def lambda_handler(event, context):
    # Extract the data from the event
    data = json.loads(event['body'])
    voucherNo = data['voucherNo']
    narration = data['narration']
    date = data['date']
    debitTransactions = data['debitTransactions']
    creditTransactions = data['creditTransactions']

    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('project_4_table')

    # Generate a unique transaction ID
    response = table.scan(ProjectionExpression='txn_id')
 
    id_int = generate_unique_id()
    item = {
        'txn_id': 0,
        'voucherCode': id_int,
    }
  
    # Add accountType based on transactionType and amount for debit transactions
    for transaction in debitTransactions:
        id_int = generate_unique_id()
        item['txn_id'] = id_int
        item['accountNo'] = transaction['accountNo']
        item['amount'] = transaction['amount']
 # Assuming transaction type 'D' for debit
        item['accountType'] = 'D'
        print(item)
        table.put_item(Item=item)

        

    # Add accountType based on transactionType and amount for credit transactions
    for transaction in creditTransactions:
        id_int = generate_unique_id()
        item['txn_id'] = id_int
        item['accountNo'] = transaction['accountNo']
        item['amount'] = transaction['amount']
 # Assuming transaction type 'C' for Credit
        item['accountType'] = 'C'
        table.put_item(Item=item)
        print(item)



    return {
        'statusCode': 200,
        'body': json.dumps('account details loaded'),
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'

        }
    }
