import os
from datetime import datetime
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
TEST_SIZE = os.getenv('TEST_SIZE', 10)
MONGO_DB = os.getenv('MONGO_DB', 'sampleBoCW_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'sampleBoCW_collection')

default_args = {
    'owner': os.getenv('OWNER', 'airflow'),
    'depends_on_past': os.getenv('DEPEND_ON_PAST', False),
    'start_date': os.getenv('START_DATE', datetime(2022, 2, 28)),
    'retries': os.getenv("RETRIES", 1),
}
