from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from os.path import join, dirname

from settings import default_args

from google.cloud import bigquery 
import json

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

def loadJsonToBigQuery(srcJsonPath):
  '''To Load json data into bigquery '''
  #BQ_CONN_ID = 'bq_gcp_conn'
  #BQ_PROJECT = 'bocwdatapipeline'
  BQ_DATASET = 'BoCWGrievancesData'
  client = bigquery.Client()
  dataset_id = BQ_DATASET
  table_id = 'BoCWMasterTable_Ver00'

  dataset_ref = client.dataset(dataset_id)
  table_ref = dataset_ref.table(table_id)
  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  job_config.autodetect = True
  
  with open(srcJsonPath, 'r') as j:
     contents = json.loads(j.read())
  
  def replaceAllDollarsInKeys(d):
    new = {}
    for k, v in d.items():
        k = k.encode('raw-unicode-escape').split(b'\\u')[0].decode()
        if isinstance(v, dict):
            v = replaceAllDollarsInKeys(v)
        new[k.replace('$', '_')] = v
    return new
  parsedJsonArray = [replaceAllDollarsInKeys(eaDoc) for eaDoc in contents]
   
  with open(srcJsonPath, 'w') as f:
    f.write('\n'.join(map(json.dumps, parsedJsonArray)))
    
  with open(srcJsonPath, "rb") as source_file:
    job = client.load_table_from_file(source_file, 
            table_ref, 
            location="asia-south1",  # Must match the destination dataset location.
            job_config=job_config)  # API request

  job.result()  # Waits for table load to complete.


dag = DAG('BoCWDataPipeline_Dev', default_args=default_args, schedule_interval=None)

fetchMongoDBData = BashOperator(task_id='DailyGrievanceDataFetch', bash_command='bash /opt/airflow/bashScripts/dumpMongoData.sh ', dag=dag )

loadDataToBigQuery = PythonOperator(task_id='DailyLoadToBigQuery', 
                                    python_callable=loadJsonToBigQuery, 
                                    op_kwargs={ 'srcJsonPath': '/opt/airflow/data/dailyMongoDump/snapShot.json'}, dag=dag)






fetchMongoDBData >> loadDataToBigQuery


