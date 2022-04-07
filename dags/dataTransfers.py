import pandas 
import json

from google.cloud import bigquery 

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

import gspread

from constants import BaseDailyDumpPath
from dataFormatting import parseJsonFile
from dotenv import load_dotenv

class DataTransfer:    

    def __init__( self, cobDate, envPath, tableId ):
        self.cobDate = cobDate
        self.tableId = tableId 
        self.envPath = envPath 

    def loadEnv(self, envPath):
        load_dotenv(envPath)

    def fetchDataDumpFileName(self, taskName, fileType='.csv', sheetName=None):
        return sheetName + '_' + taskName.split('_')[1] + 'SnapShot' + fileType if sheetName else taskName.split('_')[1] + 'SnapShot' + fileType

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
        tableId = self.tableId

        job_config = bigquery.LoadJobConfig()
        
        job = client.load_table_from_dataframe(srcDFrame, tableId, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        # table = client.get_table(table_id)  # Make an API request.
        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )

    def loadDataFrameToBigquery(self, srcCSVPaths ):
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
        tableId = self.tableId
        srcDFrame = pandas.concat([pandas.read_csv(eaCSV) for eaCSV in srcCSVPaths ])
        job_config = bigquery.LoadJobConfig()
        
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
        gc = gspread.service_account()
        for eaSheet in wsNames:
            workSheet = gc.open(gsUrl).worksheet(eaSheet)
            dFrame = pandas.DataFrame(workSheet.get_all_records())
            srcCSVPath = self.fetchDataDumpFileName(taskName, sheetName=eaSheet)
            dFrame.to_csv(srcCSVPath)
        return True

class CMSTransfer( DataTransfer ):

    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(CMSTransfer, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

    def checkInputs(self, **kwargs):
        return True

    def fetchDataFromSource( self, taskName ):
        """To fetch CMS data from MongoDB or AWS Document DB"""
        #ToDO: Pass taskName as input to bash script to create temp dump file name
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
        return PythonOperator(task_id=taskName, python_callable = self.loadJsonToBigquery, op_kwargs={ 'taskName': taskName})    

class RSTTransfer(CMSTransfer):
    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(RSTTransfer, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
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
        return BashOperator(task_id=taskName, bash_command='bash /opt/airflow/bashScripts/dumpRSTMongoData.sh ' )

    def loadDataToBigquery(self, taskName):
        """To load RST data into Bigquery"""
        return PythonOperator(task_id=taskName, python_callable = self.loadJsonToBigquery, op_kwargs={ 'taskName': taskName })

class CallerTransfer( DataTransfer ):
    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(CallerTransfer, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

    def fetchDataFromCSV(self, taskName):
        """ To fetch data from Exotel"""
        #Temporarily getting it from csv dump
        # Todo: Probably needs to be some logic to fetch from Exotel API
        srcCSVPath = self.fetchDataDumpFileName(taskName)
        callerData =  pandas.read_csv(srcCSVPath)
        return 'Done'

    def fetchDataFromSource( self, taskName ):
        """To fetch Caller data from Exotel"""
        srcCSVPath = self.fetchDataDumpFileName(taskName)        
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromCSV, op_kwargs = { 'srcCSVPath' : srcCSVPath} )

    def loadDataToBigquery(self, taskName):
        """To load Caller data to Bigquery"""
        srcCSVPath = self.fetchDataDumpFileName(taskName)
        return PythonOperator(task_id=taskName, python_callable = self.loadCSVToBigquery, op_kwargs = { 'srcCSVPath' : srcCSVPath} )

class AssigneeTransfer( CallerTransfer ):
    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs    

    def loadDataToBigquery(self, taskName):
        """To load Caller data to Bigquery"""
        srcCSVPath = self.fetchDataDumpFileName(taskName, self.kwargs['sheetName'][0])
        return PythonOperator(task_id=taskName, python_callable = self.loadCSVToBigquery, op_kwargs = { 'srcCSVPath' : srcCSVPath} )

    def fetchDataFromSource( self, taskName ):
        """To fetch Assignee data from GS"""
        opKwargs = {'taskName' : taskName, 'gsUrl': self.kwargs['gsUrl'], 'wsNames': self.kwargs['sheetName']}
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromGS, op_kwargs = opKwargs )

class CallStatusTransfer(AssigneeTransfer):
    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

class DistrictsTransfer(AssigneeTransfer):
    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(DistrictsTransfer, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

    def fetchDataFromSource( self, taskName ):
        """To fetch district data from each sheet of google workbook """
        opKwargs = {'taskName' : taskName, 'gsUrl': self.kwargs['gsUrl'], 'wsNames': self.kwargs['sheetName']}
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromGS, op_kwargs = opKwargs )

    def loadDataToBigquery(self, taskName):
        """To load District data to Bigquery"""
        srcCSVPaths = [self.fetchDataDumpFileName(taskName, sheetName=eaSheet) for eaSheet in self.kwargs['sheetName']]
        return PythonOperator(task_id=taskName, python_callable = self.loadDataFrameToBigquery, op_kwargs = { 'srcCSVPaths' : srcCSVPaths} )

class GrievanceStatusTransfer(AssigneeTransfer):
    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(GrievanceStatusTransfer, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

class BoCWDataView(DataTransfer):

    def __init__(self, cobDate, envPath, tableId, **kwargs):
        super(BoCWDataView, self).__init__( cobDate, envPath, tableId )
        #assert self.checkInputs(**kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs
        self.loadEnv( envPath )

    def checkInputs(self, **kwargs):
        return True

    def loadEnv(self, envPath):
        load_dotenv(envPath)

    def createTableInBq(self, *args, **kwargs):
        """To run data processing queries in BQ to create required views from Central Storage Datasets"""
        return True

    def fetchTableFromBq(self, *args, **kwargs):
        return True

    def loadTabletoGS(self, *args, **kwargs):
        return True        