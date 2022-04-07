from airflow.models import Variable
from airflow import DAG

from airflow.operators.python import PythonOperator
from settings import default_args

from datetime import datetime
from os.path import join, dirname

from dataTransfers import DataTransfer, CMSTransfer, RSTTransfer, CallerTransfer, AssigneeTransfer, CallStatusTransfer, DistrictsTransfer, GrievanceStatusTransfer, BoCWDataView
from constants import dataTransferConfig

tablesToUpdate = Variable.get('BoCWCentralStorageTables').split(';')
#{'BoCWCentralStorageTables': 'CMS;RST;Caller;Assignee;CallStatus;Officials;Status'}

dataTransferConfigMap = {'CMS' : dataTransferConfig(CMSTransfer, {'taskName': 'CMSfromMongo'}),
                         'RST': dataTransferConfig(RSTTransfer, {'taskName': 'RSTfromMongo'}),
                         'Caller' : dataTransferConfig(CallerTransfer, {'taskName': 'CallerFromExotel'}),
                         'Assignee': dataTransferConfig(AssigneeTransfer, {'taskName': 'AssigneeFromGS',
                                                                           'gsUrl': 'https://docs.google.com/spreadsheets/d/1rNYmGNF2dCXlPreQcu_A5-BWIO5j6MEGXHxTKVw-r1g/edit?usp=sharing', 
                                                                           'sheetName': ['Assignee']}),
                         'CallStatus': dataTransferConfig(CallStatusTransfer, {'taskName': 'CallStatusFromGS',
                                                                               'gsUrl': 'https://docs.google.com/spreadsheets/d/1twcFeCkNbGy_E4xw_LTlnBl-jVEwVX4k5LjAatqH3Jk/edit#gid=0',
                                                                               'sheetName': ['CallStatus']}),
                         'Districts' : dataTransferConfig(DistrictsTransfer, {'taskName': 'DistrictsFromGS',
                                                                              'sheetName': ['north', 'east', 'west', 'south', 'north-east', 'north-west', 'south-west', 'central', 'newdelhi'], 
                                                                              'gsUrl': 'https://docs.google.com/spreadsheets/d/1ks1Ayf3ZmZqaLtQjp0N2jvF0fv3hcrPnQS-QZl27-JA/edit#gid=1278667506'}),
                         'GrievanceStatus': dataTransferConfig(GrievanceStatusTransfer, {'taskName': 'GrievanceStatusFromGS',
                                                                       'gsUrl': 'https://docs.google.com/spreadsheets/d/1G24JHFEYbxiq518nHgjwKuplH4OQDAA-r6LkAy91W1c/edit#gid=1278667506',
                                                                       'sheetName': ['GrievanceStatus']})
                        }
                         
cobDate = datetime.utcnow().date()
dotenvPathCentralStorage = join(dirname(__file__), '.envCentralStorage')
dotenvPathBoCWProject = join(dirname(__file__), '.envBoCWProject')

with DAG('BoCWDailyDataUpdate', default_args=default_args, schedule_interval=None) as dag:
# DAG for the daily update of data from App and Google Sheets into BigQuery Central Storage
    for eaTable in tablesToUpdate:
        inputArgs = dataTransferConfigMap[eaTable].inputArgs
        taskTag = inputArgs['taskName']
        dataTransferHandle = dataTransferConfigMap[eaTable].className(cobDate, dotenvPathCentralStorage, eaTable, **inputArgs)
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
        eaInputArgs = {'datasetId': eaDataSet, 'localDataPath' : snapShotName}
        dataViewHandle = BoCWDataView(cobDate, dotenvPathBoCWProject, eaTable, **eaInputArgs)
        createTableInBq = dataViewHandle.createTableInBq()
        fetchTableFromBq = dataViewHandle.fetchTableFromBq()
        loadDatatoGS = dataViewHandle.loadTabletoGS()

        #fetchDataFromTable = PythonOperator( task_id = '_'.join(['fetch', eaDataSet, eaTable]), python_callable = fetchTablefromBq, op_kwargs=eaOpKwargs, dag = eaDag)
        #loadDatatoGS = PythonOperator(task_id = '_'.join([ eaDataSet, eaTable,'toGS']), python_callable = loadTableDatatoGS, op_kwargs = eaOpKwargs, dag=eaDag)
        createTableInBq >> fetchTableFromBq >> loadDatatoGS