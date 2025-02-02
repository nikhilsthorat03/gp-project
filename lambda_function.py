import boto3
import pandas as pd
import io
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3')


def mask_data(data):
    """
    Applies masking to sensitive data fields in a DataFrame.

    :param data: Pandas DataFrame containing sensitive information.
    :return: DataFrame with masked data.
    """
    # Mask Name by hashing it and keeping a unique identifier
    data['Name'] = data['Name'].apply(lambda x: f"User_{hash(x) % 100000}")

    # Mask Email by obfuscating the username part
    data['Email'] = data['Email'].apply(lambda x: x.split('@')[0][0] + '****@' + x.split('@')[1])

    # Mask Phone by keeping only the last 8 digits
    data['Phone'] = data['Phone'].apply(lambda x: '***-***-' + x[-8:] if len(x) > 8 else '***-***-****')

    # Mask Address by keeping only the city and state (last two parts)
    data['Address'] = data['Address'].apply(lambda x: ', '.join(x.split(',')[-2:]).strip() if ',' in x else '****')

    # Mask Credit Card Number by hiding all but the last 4 digits
    data['CreditCardNumber'] = data['CreditCardNumber'].astype(str).apply(lambda x: '*' * (len(x) - 4) + x[-4:])

    # Completely mask Bank Account details
    data['BankAccount'] = data['BankAccount'].apply(lambda x: '***************')

    # Mask Medical Record Number by keeping only the last 4 digits
    data['MedicalRecordNumber'] = data['MedicalRecordNumber'].astype(str).apply(lambda x: '*' * (len(x) - 4) + x[-4:])

    # Mask Diagnosis information based on sensitive conditions
    data['Diagnosis'] = data['Diagnosis'].apply(
        lambda x: 'Chronic Condition' if 'Diabetes' in x or 'Hypertension' in x else 'Other Condition'
    )

    # Mask Treatment Code by replacing it with a unique token
    data['TreatmentCode'] = data['TreatmentCode'].apply(lambda x: f"TOKEN_{hash(x) % 10000}")

    return data


def lambda_handler(event, context):
    """
    AWS Lambda function to:
    1. Retrieve a CSV file from S3 based on an SQS event.
    2. Perform data masking on sensitive fields.
    3. Save both masked and unmasked versions of the file back to S3 in Parquet format.

    :param event: AWS Lambda event triggered by SQS.
    :param context: AWS Lambda execution context.
    """
    try:
        # Extract message body from SQS event
        message_body = event['Records'][0]['body']
        sqs_message = eval(message_body)  # Convert string to dictionary

        # Extract bucket name and object key from the event
        bucket_name = sqs_message['Records'][0]['s3']['bucket']['name']
        object_key = sqs_message['Records'][0]['s3']['object']['key']

        # Read the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')

        # Get the current date and month for file storage paths
        current_date = pd.Timestamp.now()
        month = current_date.month  # Extract month (1-12)

        # Convert CSV content into a Pandas DataFrame
        data = pd.read_csv(io.StringIO(file_content))

        # Split data into different categories
        personal = data[['RecordID', 'Name', 'Email', 'Phone', 'Address']]
        PII = data[['RecordID', 'CreditCardNumber', 'BankAccount']]
        health = data[['RecordID', 'MedicalRecordNumber', 'Diagnosis', 'TreatmentCode']]

        # Save unmasked data in Parquet format
        def save_parquet(df, prefix):
            """
            Saves DataFrame as a Parquet file in an S3 bucket.

            :param df: DataFrame to save.
            :param prefix: S3 folder prefix for categorization.
            """
            object_path = f"{prefix}/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            s3_client.put_object(Bucket='processed-data-gp', Key=object_path, Body=parquet_buffer.getvalue())

        # Store unmasked data in respective S3 paths
        save_parquet(data, "unmasked")
        save_parquet(personal, "personal_unmasked")
        save_parquet(PII, "PII_unmasked")
        save_parquet(health, "Health_unmasked")

        # Apply data masking
        masked_data = mask_data(data)
        personal_masked = masked_data[['RecordID', 'Name', 'Email', 'Phone', 'Address']]
        PII_masked = masked_data[['RecordID', 'CreditCardNumber', 'BankAccount']]
        health_masked = masked_data[['RecordID', 'MedicalRecordNumber', 'Diagnosis', 'TreatmentCode']]

        # Store masked data in respective S3 paths
        save_parquet(masked_data, "masked")
        save_parquet(personal_masked, "personal_masked")
        save_parquet(PII_masked, "PII_masked")
        save_parquet(health_masked, "Health_masked")

        # Log completion
        current_timestamp = datetime.now()
        print(f"Data Masking Lambda completed successfully at {current_timestamp}")

    except Exception as e:
        print(f"Error processing file {object_key}: {e}")
