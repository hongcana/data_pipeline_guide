import boto3
import psycopg2
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_devcourse_redshift", "database")
user = parser.get("aws_devcourse_redshift", "username")
password = parser.get("aws_devcourse_redshift", "password")
host = parser.get("aws_devcourse_redshift", "host")
port = parser.get("aws_devcourse_redshift", "port")

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
cur = rs_conn.cursor()
sql = "SELECT * FROM Hongcana.country_info ORDER BY population;"
cur.execute(sql)
result = cur.fetchall()

for i in result:
    print(i)

cur.close()
rs_conn.close() 