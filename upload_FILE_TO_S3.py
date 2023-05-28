import pymysql
import csv
import boto3
import configparser

parser = configparser.ConfigParser()
parser.read('pipeline.conf')
access_key = parser.get("aws_boto_credentials","access_key")
secret_key = parser.get("aws_boto_credentials","secret_key")
bucket_name = parser.get("aws_boto_credentials","bucket_name")

s3 = boto3.client('s3', aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

s3_file = ["session_timestamp.csv", "session_transaction.csv", "user_session_channel.csv"]

for i in s3_file:
    s3.upload_file(i, bucket_name, i)