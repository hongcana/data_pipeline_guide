import pymysql
import configparser

parser = configparser.ConfigParser()
parser.read('pipeline.conf')
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname, user=username, password=password, db=dbname, port=int(port))

if conn is None:
    print("Error connecting to the MySQL DB.")
else:
    print("MySQL connection established!")

m_query = """
CREATE TABLE distinct_orders AS
SELECT DISTINCT OrderId, OrderStatus, LastUpdated
FROM ORDERS;
TRUNCATE TABLE Orders;
INSERT INTO Orders
SELECT * FROM distinct_orders;
DROP TABLE distinct_orders;
SELECT * FROM Orders;
"""

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()
print(results)