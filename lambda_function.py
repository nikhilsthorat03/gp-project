import pandas as pd

def lambda_handler(event, context):
    print("Hello CI-CD successfully.")
    return {
        'statusCode': 200,
        'body': 'Lets Kick Start This project Now, Thank you god!!'
    }
