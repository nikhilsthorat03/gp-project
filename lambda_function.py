import boto3
import pandas as pd
import io
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3')


def mask_data(data):
    # Apply masking logic
    data['Name'] = data['Name'].apply(lambda x: f"User_{hash(x) % 100000}")
    data['Email'] = data['Email'].apply(
        lambda x: x.split('@')[0][0] + '****@' + x.split('@')[1])
    data['Phone'] = data['Phone'].apply(
        lambda x: '***-***-' + x[-8:] if len(x) > 8 else '***-***-****')
    data['Address'] = data['Address'].apply(
        lambda x: ', '.join(x.split(',')[-2:]).strip() if ',' in x else '****')
    data['CreditCardNumber'] = data['CreditCardNumber'].astype(str).apply(
        lambda x: '*' * (len(x) - 4) + x[-4:])
    data['BankAccount'] = data['BankAccount'].apply(lambda x: '***************')
    data['MedicalRecordNumber'] = data['MedicalRecordNumber'].astype(str).apply(
        lambda x: '*' * (len(x) - 4) + x[-4:])
    data['Diagnosis'] = data['Diagnosis'].apply(
        lambda x: 'Chronic Condition' if 'Diabetes' in x or 'Hypertension' in x else 'Other Condition')
    data['TreatmentCode'] = data['TreatmentCode'].apply(
        lambda x: f"TOKEN_{hash(x) % 10000}")
    return data


def lambda_handler(event, context):
    # print(event)

    message_body = event['Records'][0]['body']
    # print(message_body)
    sqs_message = eval(message_body)  # Convert string to dictionary
    # print(sqs_message)
    bucket_name = sqs_message['Records'][0]['s3']['bucket']['name']
    # print(bucket_name)
    object_key = sqs_message['Records'][0]['s3']['object']['key']
    # print(object_key)

    # Read the CSV file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    # print(response)
    file_content = response['Body'].read().decode('utf-8')

    current_date = pd.Timestamp.now()

    # Extract date, month, and day
    date = current_date.date()  # Returns only the date part (YYYY-MM-DD)
    month = current_date.month  # Extracts the month (1-12)

    data = pd.read_csv(io.StringIO(file_content))
    personal = data[['RecordID', 'Name', 'Email', 'Phone', 'Address']]
    PII = data[['RecordID', 'CreditCardNumber', 'BankAccount']]
    health = data[['RecordID', 'MedicalRecordNumber', 'Diagnosis', 'TreatmentCode']]

    masked_object_key = f"unmasked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    data.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    masked_object_key = f"personal_unmasked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    personal.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    masked_object_key = f"PII_unmasked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    PII.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    masked_object_key = f"Health_unmasked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    health.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    # Mask the data
    masked_data = mask_data(data)
    personal = masked_data[['RecordID', 'Name', 'Email', 'Phone', 'Address']]
    PII = masked_data[['RecordID', 'CreditCardNumber', 'BankAccount']]
    health = masked_data[['RecordID', 'MedicalRecordNumber', 'Diagnosis', 'TreatmentCode']]
    # print(masked_data.head(5))

    # Save the masked file back to S3 in Parquet format
    masked_object_key = f"masked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    data.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    masked_object_key = f"personal_masked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    personal.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    masked_object_key = f"PII_masked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    PII.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    masked_object_key = f"Health_masked/2025/{month}/{object_key.split('/')[-1].replace('.csv', '.parquet')}"
    parquet_buffer = io.BytesIO()
    health.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='processed-data-gp', Key=masked_object_key, Body=parquet_buffer.getvalue())

    current_timestamp = datetime.now()
    # print(f"Masked file saved to s3://{'processed-data-gp'}/{masked_object_key}")
    print(f"Data Masking Lambda completed -- successfully {current_timestamp}")