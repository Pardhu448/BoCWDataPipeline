from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from settings import default_args

from dotenv import load_dotenv
from os.path import join, dirname
from datetime import datetime

from dataTransfers import DataTransfer, CMSTransfer, RSTTransfer, CallerTransfer, AssigneeTransfer, CallStatusTransfer, OfficialsTransfer, StatusTransfer
from constants import dataTransferConfig

from dataTransfers import fetchTablefromBq, loadTableDatatoGS

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

tablesToUpdate = Variable.get('BoCWCentralStorageTables').split(';')
#{'BoCWCentralStorageTables': 'CMS;RST;Caller;Assignee;CallStatus;Officials;Status'}

dataTransferConfigMap = {'CMS' : dataTransferConfig(CMSTransfer, {'mongoDumpPath': 'cmsSnapShot.json', 'taskName': 'CMSDatafromMongo'}),
                         'RST': dataTransferConfig(RSTTransfer, {'mongoDumpPath': 'rstSnapShot.json', 'taskName': 'RSTDatafromMongo'}),
                         'Caller' : dataTransferConfig(CallerTransfer, {'csvDumpPath': 'exotelCallerSnapShot.csv', 
                                                                        'taskName': 'CallerDataFromExotel',
                                                                        'gsUrl': '',
                                                                        'sheetName': 'Callers'}),
                         'Assignee': dataTransferConfig(AssigneeTransfer, {'csvDumpPath': 'assigneeSnapShot.csv', 
                                                                           'taskName': 'AssigneeDataFromGS',
                                                                           'gsUrl': '', 
                                                                           'sheetName': 'Assignee'}),
                         'CallStatus': dataTransferConfig(CallStatusTransfer, {'csvDumpPath': 'callSatusSnapShot.csv', 
                                                                               'taskName': 'CallStatusDataFromGS',
                                                                               'gsUrl': '',
                                                                               'sheetName': 'CallStatus'}),
                         'Officials' : dataTransferConfig(OfficialsTransfer, {'csvDumpPath': 'officialsSnapShot.csv', 
                                                                              'taskName': 'OfficialsDataFromGS', 
                                                                              'gsUrl': '',
                                                                              'sheetName': 'Officials'}),
                         'GrievanceStatus': dataTransferConfig(StatusTransfer, {'csvDumpPath': 'grievanceStatusSnapShot.csv', 
                                                                       'taskName': 'GrievanceStatusDataFromGS',
                                                                       'gsUrl': '',
                                                                       'sheetName': 'GrievanceStatus'}) }

cobDate = datetime.utcnow().date()

with DAG('BoCWDailyDataUpdate', default_args=default_args, schedule_interval=None) as dag:
# DAG for the daily update of data from App and Google Sheets into BigQuery Central Storage        
    for eaTable in tablesToUpdate:
        inputArgs = dataTransferConfigMap[eaTable].inputArgs
        taskTag = inputArgs['taskName']
        dataTransferHandle = dataTransferConfigMap[eaTable].className(cobDate, eaTable, **inputArgs)
        fetchDataFromSource = dataTransferHandle.fetchDataFromSource('_'.join(['Fetch', taskTag]))
        loadDataToBigquery = dataTransferHandle.loadDataToBigquery('_'.join(['Load', taskTag]))
        fetchDataFromSource >> loadDataToBigquery

# Dags to provide views based on the filtering criteria of IA associsates or Officials
# Once the filtering criteria is configured, filtered data is made available by updating 
# data into Google Sheets. The updation interval depends on the date range of filtering criteria
# and could be either Monthly or Qurterly

dagIA = DAG('GetIADataset', default_args = default_args, schedule_interval = None)
dagOfficials = DAG('GetOfficialsDataset', default_args = default_args, schedule_interval = None)

dagMap = {'IAAssocisates': dagIA, 'GovtOfficials': dagOfficials}

tablesToUpdate = {'IAAssocisates': Variable.get('IADatasetTables'), 'GovtOfficials': Variable.get('OfficialsDatasetTables')}
#{'IADatasetTables': 'CMSFiltered;RSTFiltered;CallerFiltered;AssigneeFiltered;CallStatusFiltered'}
#{'OfficialsDatasetTables': 'Districts;GrievanceStatus'}

for eaDataSet, eaTableList in tablesToUpdate.items():
    for eaTable in eaTableList.split(';'):
        eaDag = dagMap[eaDataSet]
        snapShotName = 'Districts_SnapSot.xlsx'  if eaTable == 'Districts' else '_'.join([eaTable, 'SnapShot.csv'])
        eaOpKwargs={'datasetId': eaDataSet, 'tableId': eaTable, 'localDataPath' : snapShotName}
        fetchDataFromTable = PythonOperator( task_id = '_'.join(['fetch', eaDataSet, eaTable]), python_callable = fetchTablefromBq, op_kwargs=eaOpKwargs, dag = eaDag)
        loadDatatoGS = PythonOperator(task_id = '_'.join([ eaDataSet, eaTable,'toGS']), python_callable = loadTableDatatoGS, op_kwargs = eaOpKwargs, dag=eaDag)
        fetchDataFromTable >> loadDatatoGS