from dotenv import load_dotenv
import oracledb
import glob
import os

load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_hostname = os.getenv('DB_HOSTNAME')
db_port = os.getenv('DB_PORT')
db_service_name = os.getenv('DB_SERVICE_NAME')

dsn = oracledb.makedsn(db_hostname, db_port, service_name=db_service_name)

connection = oracledb.connect(
    user=db_user,
    password=db_password,
    dsn=dsn
)

cursor = connection.cursor()

view_files = glob.glob('*.sql')

for view_file in view_files:
    with open(view_file, 'r') as file:
        view_query = file.read()
        
        try:
            cursor.execute(view_query)
            
            results = cursor.fetchall()
            
            print(f"Results for {view_file}:")
            for row in results:
                print(row)
            
            print(f"Successfully executed query from {view_file}")
        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Error executing query from {view_file}: {error.message}")

cursor.close()
connection.close()