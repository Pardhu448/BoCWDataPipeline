from airflow.models import Variable
from airflow import DAG

from airflow.operators.python import PythonOperator
from settings import default_args

from datetime import datetime
from os.path import join, dirname

from dataTransfers import DataTransfer, CMSTransfer, RSTTransfer, CallerTransfer, AssigneeTransfer, CallStatusTransfer, DistrictsTransfer, GrievanceStatusTransfer, BoCWDataView
from constants import dataTransferConfig, BqIAAssociatesViewDataSet, BqOfficialsViewDataSet
from dataSchema import districtsSchema, grievanceStatusSchema
from utils import getScheduleInterval 

tablesToUpdate = Variable.get('BoCWCentralStorageTables').split(';')
#{'BoCWCentralStorageTables': 'CMS;RST;Caller;Assignee;CallStatus;Officials;Status'}

dataTransferConfigMap = {'CMS' : dataTransferConfig(CMSTransfer, {'taskName': 'CMSfromMongo'}),
                         'RST': dataTransferConfig(RSTTransfer, {'taskName': 'RSTfromMongo'}),
                         'Caller' : dataTransferConfig(CallerTransfer, {'taskName': 'CallerFromExotel'}),
                         'Assignee': dataTransferConfig(AssigneeTransfer, {'taskName': 'AssigneeFromGS',
                                                                           'gsUrl': 'https://docs.google.com/spreadsheets/d/1rNYmGNF2dCXlPreQcu_A5-BWIO5j6MEGXHxTKVw-r1g/edit#gid=0', 
                                                                           'sheetName': ['Assignee']}),
                         'CallStatus': dataTransferConfig(CallStatusTransfer, {'taskName': 'CallStatusFromGS',
                                                                               'gsUrl': 'https://docs.google.com/spreadsheets/d/1twcFeCkNbGy_E4xw_LTlnBl-jVEwVX4k5LjAatqH3Jk/edit#gid=0',
                                                                               'sheetName': ['CallStatus']}),
                         'Districts' : dataTransferConfig(DistrictsTransfer, {'taskName': 'DistrictsFromGS',
                                                                                'schema': districtsSchema,
                                                                              'sheetName': ['north', 'east', 'west', 'south', 'north-east', 'north-west', 'south-west', 'central', 'newdelhi'], 
                                                                              #'gsUrl': 'https://docs.google.com/spreadsheets/d/1dmZOwFQIMVkY7aLjNTDRl5bfMjzIFPAojy05EvjEcWU/edit#gid=1278667506'}),
                                                                              'gsUrl': 'https://docs.google.com/spreadsheets/d/1ks1Ayf3ZmZqaLtQjp0N2jvF0fv3hcrPnQS-QZl27-JA/edit#gid=1278667506'}),
                         'GrievanceStatus': dataTransferConfig(GrievanceStatusTransfer, {'taskName': 'GrievanceStatusFromGS',
                                                                                        'schema': grievanceStatusSchema,
                                                                       'gsUrl': 'https://docs.google.com/spreadsheets/d/1G24JHFEYbxiq518nHgjwKuplH4OQDAA-r6LkAy91W1c/edit#gid=1278667506',
                                                                       'sheetName': ['north', 'east', 'west', 'south', 'north-east', 'north-west', 'south-west', 'central', 'newdelhi']})
                        }
                         
cobDate = datetime.utcnow()
dotenvPathCentralStorage = join(dirname(__file__), '.envCentralStorage')
dotenvPathBoCWProject = join(dirname(__file__), '.envBoCWProject')
#dailyUpdateTime = '0 2 * * *'
dailyUpdateTime = '@once'

with DAG('BoCWDailyDataUpdate', default_args=default_args, start_date = cobDate, schedule_interval=dailyUpdateTime) as dag:
# DAG for the daily update of data from App and Google Sheets into BigQuery Central Storage
    for eaTable in tablesToUpdate:
        inputArgs = dataTransferConfigMap[eaTable].inputArgs
        taskTag = inputArgs['taskName']
        dataTransferHandle = dataTransferConfigMap[eaTable].className(cobDate.date(), dotenvPathCentralStorage, tableId=eaTable, **inputArgs)
        fetchDataFromSource = dataTransferHandle.fetchDataFromSource('_'.join(['Fetch', taskTag]))
        loadDataToBigquery = dataTransferHandle.loadDataToBigquery('_'.join(['Load', taskTag]))
        fetchDataFromSource >> loadDataToBigquery

# Dags to provide views based on the filtering criteria of IA associsates or Officials
# Once the filtering criteria is configured, filtered data is made available by updating 
# data into Google Sheets. The updation interval depends on the date range of filtering criteria
# and could be either daily or Monthly or Qurterly

# Fetch all configs defined for data view tasks in configurations file
from dataViewConfig import DataViewDagConfigMap

for _, eaDagConfig in DataViewDagConfigMap().items():
    dagID = eaDagConfig.DagName
    eaDag = DAG(dagID, default_args=default_args, start_date = cobDate, schedule_interval=getScheduleInterval(eaDagConfig.Schedule))
    globals()[dagID] = eaDag
    opKwargs = {'dagName': eaDagConfig.DagName, 'dag': eaDag, 'dataViewConfig': eaDagConfig.DataViewConfig}
    dataViewHandle = BoCWDataView(cobDate.date(), dotenvPathBoCWProject, **opKwargs)
    #createTableInBq = dataViewHandle.createTableViewInBq('CreateTableView')
    fetchTableFromBq = dataViewHandle.fetchTableDataFromBq('FetchTableFromBq')
    loadDatatoGS = dataViewHandle.loadTableDatatoGS('LoadDataInGS')

    #createTableInBq >> fetchTableFromBq >> loadDatatoGS
    fetchTableFromBq >> loadDatatoGS