import boto3
import pandas as pd
import io

# Configuration
BUCKET_NAME = 'bro-data-bucket-demo'
RAW_KEY = 'messy/bro_data.csv'
CLEANED_KEY = 'cleaned/cleaned_bro_data.csv'

# Create an S3 client (make sure your AWS credentials are configured)
s3 = boto3.client('s3', region_name='us-east-1')

# Download the CSV file from S3 into memory
response = s3.get_object(Bucket=BUCKET_NAME, Key=RAW_KEY)
raw_data = response['Body'].read()

# Use StringIO to allow pandas to read the CSV data from memory
data_io = io.StringIO(raw_data)
df = pd.read_csv(data_io, skipinitialspace=True)

# Clean column names: strip extra spaces and convert to lower-case
df.columns = [col.strip().lower() for col in df.columns]

# Remove extra quotes and whitespace from each string column
for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].str.strip().str.replace('"', '', regex=False)

# Drop rows with missing critical fields (adjust as necessary)
df_clean = df.dropna(subset=['timestamp', 'source_ip', 'destination_ip', 'protocol'])

# Convert the timestamp field into a datetime object, dropping rows that cannot be parsed
df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
df_clean = df_clean.dropna(subset=['timestamp'])

# (Optional) Print the cleaned DataFrame to verify
print(df_clean.head())

# Save the cleaned data to a CSV in memory
cleaned_data_io = io.StringIO()
df_clean.to_csv(cleaned_data_io, index=False)
cleaned_data_str = cleaned_data_io.getvalue()

# Re-upload the cleaned CSV back to S3
s3.put_object(Bucket=BUCKET_NAME, Key=CLEANED_KEY, Body=cleaned_data_str)
print(f"Cleaned data uploaded to s3://{BUCKET_NAME}/{CLEANED_KEY}")
