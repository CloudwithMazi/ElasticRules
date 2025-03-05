import boto3
import pandas as pd
import io 

BUCKET_NAME = 'tier1-data-dump'
RAW_KEY = '/messy/bro_data.csv'
CLEAN_KEY = '/clean/cleaned_bro_data.csv'

s3 = boto3.client('s3') 