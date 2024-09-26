from dotenv import load_dotenv
from datetime import datetime
import oracledb
import logging
import glob
import os

load_dotenv()

log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
    
log_filename = os.path.join(log_dir, datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

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

view_folders = glob.glob(os.path.join('Views', 'View*'))

for view_folder in view_folders:
    view_files = glob.glob(os.path.join(view_folder, '*.sql'))
    working_bool = True
    for view_file in view_files:
        with open(view_file, 'r') as file:
            view_query = file.read()
            logging.info(f"{view_folder}:")
            try:
                cursor.execute(view_query[:-1])
                
                logging.info(f"[{view_file}] Query executed successfully")
            except oracledb.DatabaseError as e:
                working_bool = False
                error, = e.args
                logging.info(f"[{view_file}] Error executing query: {error.message}")

    output_dir = os.path.split(view_folder)
    output_filename = output_dir[-1].split('_')
    if working_bool:
        logging.info(f"{view_folder}: All queries executed successfully")
    else:
        output_dir[-1] = "InProgress_" + '_'.join(output_filename[:1])
        
    os.makedirs(os.path.join(*output_dir))
    
cursor.close()
connection.close()