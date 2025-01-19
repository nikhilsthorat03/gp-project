import pandas as pd

def lambda_handler(event, context):
    print("Hello CI-CD successfully.")
    return {
        'statusCode': 200,
        'body': 'Hello World GG You are great!Finally we did it... too GOATED to do so....'
    }
