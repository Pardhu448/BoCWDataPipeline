import pandas 
import json

from google.cloud import bigquery 

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

import gspread

from constants import BaseDailyDumpPath, BqCentralStorageDataSet, BqProjectLocation
from dataFormatting import parseJsonFile
from dotenv import load_dotenv
import os 
import numpy 

from sql.bqSqlQueries import queryDateFilteredData

class DataTransfer:    

    def __init__( self, cobDate, envPath ):
        self.cobDate = cobDate
        self.envPath = envPath 

    def loadEnv(self, envPath):
        load_dotenv(envPath)

    def fetchRefinedTableID(self):
        storageProject = os.environ.get("BqStorageProject")
        dataSet = BqCentralStorageDataSet
        return '.'.join([storageProject, dataSet, self.tableId])

    def fetchDataDumpFileName(self, taskName, fileType='.csv', sheetName=None):
        fileName = taskName.split('_')[1] + 'SnapShot' + fileType if sheetName else taskName.split('_')[1] + 'SnapShot' + fileType
        return '/'.join([BaseDailyDumpPath, fileName])

    def loadCSVToBigquery(self, srcCSVPath):
        """To load CSV data to Bigquery"""
        # DesignPointToNote: we are appending data daily and this might 
        # cause redundant data if there are no daily changes.
        # Solution would be to clean up the Table periodically. Cleaning up 
        # invovles removing duplicates by keeping the latest snapshot.
        # Based on best practices suggested by google, following are the steps invovled:
        # 1. Create a new table (with temp name - 'TempName') from the 'OldTable' by removing duplicates
        # 2. Delete the 'OldTable' and rename the new table with 'OldTable' name  
        
        # Construct a BigQuery client object
        client = bigquery.Client()
        srcDFrame = pandas.read_csv(srcCSVPath)
        tableId = self.fetchRefinedTableID()

        job_config = bigquery.LoadJobConfig()
        
        job = client.load_table_from_dataframe(srcDFrame, tableId, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        # table = client.get_table(table_id)  # Make an API request.
        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )

    def loadDataFrameToBigquery(self, srcCSVPaths, schema=None ):
        """To load dataframe data to Bigquery"""
        # DesignPointToNote: we are appending data daily and this might 
        # cause redundant data if there are no daily changes.
        # Solution would be to clean up the Table periodically. Cleaning up 
        # invovles removing duplicates by keeping the latest snapshot.
        # Based on best practices suggested by google, following are the steps invovled:
        # 1. Create a new table (with temp name - 'TempName') from the 'OldTable' by removing duplicates
        # 2. Delete the 'OldTable' and rename the new table with 'OldTable' name  
        
        # Construct a BigQuery client object
        client = bigquery.Client()
        tableId = self.fetchRefinedTableID()
        srcDFrame = pandas.concat([pandas.read_csv(eaCSV) for eaCSV in srcCSVPaths ])
        srcDFrame = srcDFrame.astype(dtype=str)
        job_config = bigquery.LoadJobConfig(schema=schema)
        
        job = client.load_table_from_dataframe(srcDFrame, tableId, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        # table = client.get_table(table_id)  # Make an API request.
        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )

    def fetchDataFromGS(self, taskName, gsUrl, wsNames):
        """ To fetch data from google sheet updated manually"""
        gc = gspread.service_account(filename = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
        for eaSheet in wsNames:
            workSheet = gc.open_by_url(gsUrl).worksheet(eaSheet)
            dFrame = pandas.DataFrame(workSheet.get_all_records())
            srcCSVPath = self.fetchDataDumpFileName(taskName, sheetName=eaSheet)
            dFrame.to_csv(srcCSVPath, index=False)
        return True

    def fetchDataTableFromBq(self, bqClient, tableID):
        """ To fetch data from big query ad save it in dump file"""
        
        return True

class CMSTransfer( DataTransfer ):

    def __init__(self, cobDate, envPath, **kwargs):
        super(CMSTransfer, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.tableId = kwargs['tableId']
        self.kwargs = kwargs

    def checkInputs(self, **kwargs):
        return True

    def fetchDataFromSource( self, taskName ):
        """To fetch CMS data from MongoDB or AWS Document DB"""
        #ToDO: Pass taskName as input to bash script to create temp dump file name
        self.loadEnv( self.envPath )
        return BashOperator(task_id=taskName, bash_command='bash /opt/airflow/bashScripts/dumpCMSMongoData.sh ' )

    def loadJsonToBigquery(self, taskName ):
        """To load json data to bigquery"""
        #BQ_CONN_ID = 'bq_gcp_conn'
        #BQ_PROJECT = 'bocwdatapipeline'
        BQ_DATASET = 'BoCW'
        client = bigquery.Client()
        dataset_id = BQ_DATASET
        #dataSets = ['cmsSnapShot.json', 'rstSnapShot.json']
        
        srcJsonPath = self.fetchDataDumpFileName(taskName, fileType='.json') 
        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.autodetect = True
        
        contents = []
        for eaLine in open(srcJsonPath, 'r'):
            eaLine = eaLine.replace('.', '_')
            dataRecord = json.loads(eaLine)    
            dataRecord['data'] = json.loads(dataRecord['data'])    
            contents.append(dataRecord)
        parsedJsonArray = [parseJsonFile(eaDoc) for eaDoc in contents]
        
        with open(srcJsonPath, 'w') as f:
            f.write('\n'.join(map(json.dumps, parsedJsonArray)))
            
        with open(srcJsonPath, "rb") as source_file:
            table_ref = dataset_ref.table(self.tableId)
            job = client.load_table_from_file(source_file, 
                                              table_ref,
                                              location="asia-south1",  # Must match the destination dataset location
                                              job_config=job_config)  # API request

            job.result()

    def loadDataToBigquery(self, taskName):
        """To load CMS data into Bigquery"""
        self.loadEnv( self.envPath )
        return PythonOperator(task_id=taskName, python_callable = self.loadJsonToBigquery, op_kwargs={ 'taskName': taskName})    

class RSTTransfer(CMSTransfer):
    def __init__(self, cobDate, envPath, **kwargs):
        super(RSTTransfer, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.tableId = kwargs['tableId'] 
        self.kwargs = kwargs

    def loadJsonToBigquery(self, taskName ):
        """To load json data to bigquery"""
        #BQ_CONN_ID = 'bq_gcp_conn'
        #BQ_PROJECT = 'bocwdatapipeline'
        BQ_DATASET = 'BoCW'
        client = bigquery.Client()
        dataset_id = BQ_DATASET
        #dataSets = ['cmsSnapShot.json', 'rstSnapShot.json']

        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.autodetect = True

        srcJsonPath = self.fetchDataDumpFileName(taskName, fileType='.json')

        with open(srcJsonPath, 'r') as j:
            contents = json.loads(j.read())

        parsedJsonArray = [parseJsonFile(eaDoc) for eaDoc in contents]
        
        with open(srcJsonPath, 'w') as f:
            f.write('\n'.join(map(json.dumps, parsedJsonArray)))
            
        with open(srcJsonPath, "rb") as source_file:
            table_ref = dataset_ref.table(self.tableId)
            job = client.load_table_from_file(source_file, 
                                              table_ref,
                                              location="asia-south1",  # Must match the destination dataset location.
                                              job_config=job_config)  # API request

            job.result()  # Waits for table load to complete

    def fetchDataFromSource( self, taskName ):
        """To fetch CMS data from MongoDB or AWS Document DB"""
        self.loadEnv( self.envPath )
        return BashOperator(task_id=taskName, bash_command='bash /opt/airflow/bashScripts/dumpRSTMongoData.sh ' )

    def loadDataToBigquery(self, taskName):
        """To load RST data into Bigquery"""
        self.loadEnv( self.envPath )
        return PythonOperator(task_id=taskName, python_callable = self.loadJsonToBigquery, op_kwargs={ 'taskName': taskName })

class CallerTransfer( DataTransfer ):
    #Data Formatting Notes:
    #column names cannot be fancy for BigQuery- no spaces, specialCharacters
    def __init__(self, cobDate, envPath, **kwargs):
        super(CallerTransfer, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.tableId = kwargs['tableId']
        self.kwargs = kwargs

    def fetchDataFromCSV(self, srcCSVPath):
        """ To fetch data from Exotel"""
        #Temporarily getting it from csv dump
        # Todo: Probably needs to be some logic to fetch from Exotel API
        callerData =  pandas.read_csv(srcCSVPath)
        return 'Done'

    def fetchDataFromSource( self, taskName ):
        """To fetch Caller data from Exotel"""
        self.loadEnv( self.envPath )
        srcCSVPath = self.fetchDataDumpFileName(taskName)        
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromCSV, op_kwargs = { 'srcCSVPath' : srcCSVPath} )

    def loadDataToBigquery(self, taskName):
        """To load Caller data to Bigquery"""
        self.loadEnv( self.envPath )
        srcCSVPath = self.fetchDataDumpFileName(taskName)
        return PythonOperator(task_id=taskName, python_callable = self.loadCSVToBigquery, op_kwargs = { 'srcCSVPath' : srcCSVPath} )

class AssigneeTransfer( CallerTransfer ):
    def __init__(self, cobDate, envPath, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.tableId = kwargs['tableId']
        self.kwargs = kwargs    

    def loadDataToBigquery(self, taskName):
        """To load Caller data to Bigquery"""
        self.loadEnv( self.envPath )
        srcCSVPath = self.fetchDataDumpFileName(taskName, sheetName=self.kwargs['sheetName'][0])
        return PythonOperator(task_id=taskName, python_callable = self.loadCSVToBigquery, op_kwargs = { 'srcCSVPath' : srcCSVPath} )

    def fetchDataFromSource( self, taskName ):
        """To fetch Assignee data from GS"""
        self.loadEnv( self.envPath )
        opKwargs = {'taskName' : taskName, 'gsUrl': self.kwargs['gsUrl'], 'wsNames': self.kwargs['sheetName']}
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromGS, op_kwargs = opKwargs )

class CallStatusTransfer(AssigneeTransfer):
    def __init__(self, cobDate, envPath, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.tableId = kwargs['tableId']
        self.kwargs = kwargs

class DistrictsTransfer(AssigneeTransfer):
    def __init__(self, cobDate, envPath, **kwargs):
        super(DistrictsTransfer, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.tableId = kwargs['tableId']
        self.kwargs = kwargs

    def fetchDataDumpFileName(self, taskName, fileType='.csv', sheetName=None):
        fileName = sheetName + '_' + taskName.split('_')[1] + 'SnapShot' + fileType if sheetName else taskName.split('_')[1] + 'SnapShot' + fileType
        return '/'.join([BaseDailyDumpPath, fileName])

    def fetchDataFromSource( self, taskName ):
        """To fetch district data from each sheet of google workbook """
        self.loadEnv( self.envPath )
        opKwargs = {'taskName' : taskName, 'gsUrl': self.kwargs['gsUrl'], 'wsNames': self.kwargs['sheetName']}
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromGS, op_kwargs = opKwargs )

    def loadDataToBigquery(self, taskName):
        """To load District data to Bigquery"""
        self.loadEnv( self.envPath )
        srcCSVPaths = [self.fetchDataDumpFileName(taskName, sheetName=eaSheet) for eaSheet in self.kwargs['sheetName']]
        dataSchema = self.kwargs['schema']
        return PythonOperator(task_id=taskName, python_callable = self.loadDataFrameToBigquery, op_kwargs = { 'srcCSVPaths' : srcCSVPaths, 'schema': dataSchema} )

class GrievanceStatusTransfer(DistrictsTransfer):
    def __init__(self, cobDate, envPath, **kwargs):
        super(GrievanceStatusTransfer, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.tableId = kwargs['tableId']
        self.kwargs = kwargs

class BoCWDataView(DataTransfer):

    def __init__(self, cobDate, envPath, dagConfig, **kwargs):
        super(BoCWDataView, self).__init__( cobDate, envPath )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.dagInfo = dagConfig
        self.kwargs = kwargs

    def checkInputs(self, **kwargs):
        return True

    def loadEnv(self, envPath):
        load_dotenv(envPath)

    def loadViewInBq(self):
        client = bigquery.Client()
        
        #Create seperate dataset where we need to store table view
        viewDataSetID = self.dagInfo['DataViewConfig']['destDataSet']
        viewDataSet = bigquery.Dataset(viewDataSetID)
        viewDataSet.location = BqProjectLocation
        viewData = client.create_dataset(viewDataSet)  # API request

        viewTableId = self.dagInfo['DataViewConfig']['viewTableName']
        viewTable = bigquery.Table(viewData.table(viewTableId))

        #Source data info
        srcDataTableId = self.dagInfo['DataViewConfig']['sourceDataTable']
        
        #Create table with required view
        dateRange = self.dagInfo['DataViewConfig']['dateRange']
        columnFilter = self.dagInfo['DataViewConfig']['columnsRequired']
        viewTable.view_query = queryDateFilteredData(srcDataTableId, columnFilter, dateRange )
        view = client.create_table(viewTable)  # API request

    def createTableViewInBq(self, taskName, **kwargs):
        """To run data processing queries in BQ to create required views from Central Storage Datasets"""
        return PythonOperator(task_id=taskName, python_callable = self.loadViewInBq )

    def fetchTableFromBq(self, *args, **kwargs):
        return True

    def loadTabletoGS(self, *args, **kwargs):
        return True        