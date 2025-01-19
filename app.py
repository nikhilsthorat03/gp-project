import pandas as pd

def lambda_handler(event, context):
    print("Hello CI-CD successfully completed.....")
    return {
        'statusCode': 200,
        'body': 'Hello'
    }
