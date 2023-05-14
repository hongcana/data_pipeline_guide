import requests as rs
import json
import csv
import configparser
import boto3
from datetime import datetime

api_response = rs.get("http://api.open-notify.org/iss-now.json")

# json 형식으로 출력해보는 코드
# print(api_response.content)

# json 처리
response_json = json.loads(api_response.content)
datas = []

location = response_json["iss_position"]
datas.append(location["latitude"])
datas.append(location["longitude"])
datas.append(response_json["timestamp"])

print(datas)
export_file = "export_file.csv"

with open(export_file, "a", newline="") as fp:
    csvw = csv.writer(fp, delimiter="|")
    csvw.writerows([datas])  # 2차원 리스트로 처리
fp.close()


# load aws_boto_credentials value
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
s3.upload_file(export_file, bucket_name, export_file)
