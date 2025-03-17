import boto3

BUCKET_NAME = 'bro-data-bucket-demo'
REGION = 'us-east-1'

s3 = boto3.client('s3', region_name=REGION)

try:
    if REGION == 'us-east-1':
        # No LocationConstraint needed for us-east-1
        s3.create_bucket(Bucket=BUCKET_NAME)
    else:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={'LocationConstraint': REGION}
        )
    print(f"Bucket '{BUCKET_NAME}' created successfully.")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"Bucket '{BUCKET_NAME}' already exists and is owned by you.")
except Exception as e:
    print(f"Error creating bucket: {e}")
