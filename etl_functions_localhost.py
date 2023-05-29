import pymysql
import configparser
import requests

def get_mysql_connection():
    parser = configparser.ConfigParser()
    parser.read('pipeline.conf')
    hostname = parser.get("mysql_config_local", "hostname")
    port = parser.get("mysql_config_local", "port")
    username = parser.get("mysql_config_local", "username")
    dbname = parser.get("mysql_config_local", "database")
    password = parser.get("mysql_config_local", "password")

    conn = pymysql.connect(host=hostname, user=username, password=password, db=dbname, port=int(port))
    
    # 연결 자체를 return -> 추후 load 과정에서 commit을 위함
    return conn

def init_drop_create(cursor):

    # Full Refresh
    cursor.execute("DROP TABLE IF EXISTS name_gender")
    sql = """CREATE TABLE name_gender (
        name varchar(32) primary key,
        gender varchar(8)
    )"""
    cursor.execute(sql)

def extract(url):
    f = requests.get(url)
    return (f.text)

def transform(text):
    lines = text.strip().split("\n")
    records = []
    for l in lines[1:]:
        (name, gender) = l.split(",")
        records.append([name, gender])
    return records

def load(records):
    conn = get_mysql_connection()
    init_drop_create(conn.cursor())
    for r in records:
        name = r[0]
        gender = r[1]
        print(name, "-", gender)
        sql = "INSERT INTO name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
        
        # 트랜잭션 적용
        try:
            conn.cursor().execute(sql)
            conn.commit()
        except:
            conn.rollback()

parser = configparser.ConfigParser()
parser.read('pipeline.conf')
link = parser.get("mysql_config_local", "practice_csv_link")
data = extract(link)
lines = transform(data)
load(lines)
