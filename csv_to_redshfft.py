import psycopg2
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("redshift_jongwook", "dbname")
user = parser.get("redshift_jongwook", "user")
password = parser.get("redshift_jongwook", "password")
host = parser.get("redshift_jongwook", "host")
port = parser.get("redshift_jongwook", "port")

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
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# COPY command
file_path = "s3://" + bucket_name + "/DataConcat.csv"
role_string = (
    "arn:aws:iam::542670215238:role/Redshift_other_S3,arn:aws:iam::"
    + account_id
    + ":role/S3_other_account_upload_role"
)

# 커서 오픈
cur = rs_conn.cursor()

# 트랜잭션
try:
    sql = "DROP TABLE IF EXISTS raw_data.dataconcat;"
    cur.execute(sql)

    sql = """
        CREATE TABLE raw_data.dataconcat (
            Year varchar(16) primary key,
            TravelType_Individual INT,
            TravelType_PartialPackage INT,
            TravelType_FullPackage INT,
            Purpose_Lesiure INT,
            Purpose_Work INT,
            Purpose_VacationTour INT,
            Purpose_Family_Friend INT,
            Purpose_Educational INT,
            Purpose_ETC INT,
            Data_Depend varchar(16)
        );
        """
    cur.execute(sql)

    sql = "COPY raw_data.dataconcat"
    sql += " from %s"
    sql += " iam_role %s delimiter ',' removequotes;"
    cur.execute(sql, (file_path, role_string))
    rs_conn.commit()
    print("job done...")
except:
    rs_conn.rollback()
    print("transaction failed...")

sql2 = "SELECT * FROM raw_data.dataconcat;"
cur.execute(sql2)
result = cur.fetchall()  # 원하면 print해서 보면 됩니다.

cur.close()
rs_conn.close()
