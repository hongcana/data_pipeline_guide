import boto3
import psycopg2
import configparser
import json

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

# redshift cluster에 연결
rs_conn = psycopg2.connect(
    "dbname="
    + dbname
    + " user="
    + user
    + " password="
    + password
    + " host="
    + host
    + " port="
    + port
)

account_id = parser.get("aws_boto_credentials", "account_id")
iam_role = parser.get("aws_creds", "iam_role")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# COPY command
file_path = "s3://" + bucket_name + "/order_extract.csv"
role_string = "arn:aws:iam::" + account_id + ":role/" + iam_role

# 커서 오픈
cur = rs_conn.cursor()

# 전체 추출의 경우, 테이블을 잘라내지 않으면 중복이 된다.
sql = "TRUNCATE public.Orders;"
cur.execute(sql)

sql = """
    CREATE TABLE IF NOT EXISTS public.Orders (
        OrderId int,
        OrderStatus varchar(30),
        LastUpdated timestamp
    );
    """

# sql = """
#     CREATE TABLE IF NOT EXISTS public.Timelines (
#         pos_x float,
#         pos_y float,
#         LastUpdated int
#     );
#     """


sql = "COPY public.Orders"
sql += " from %s"
sql += " iam_role %s;"
cur.execute(sql, (file_path, role_string))
rs_conn.commit()

sql2 = "SELECT * FROM public.Orders;"
cur.execute(sql2)
result = cur.fetchall()
print(result)

cur.close()
rs_conn.close()
