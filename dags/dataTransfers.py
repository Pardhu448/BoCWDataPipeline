import pandas 
import json

from google.cloud import bigquery 

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

import gspread

from constants import BaseDailyDumpPath
from dataFormatting import parseJsonFile

class DataTransfer:

    def __init__( self, cobDate, tableId ):
        self.cobDate = cobDate
        self.tableId = tableId 

    def loadCSVToBigquery(self, srcCSVPath):
        """To load CSV data to Bigquery"""
        # Construct a BigQuery client object.
        client = bigquery.Client()
        srcDFrame = pandas.read_csv(srcCSVPath)
        tableId = self.tableId

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = client.load_table_from_dataframe(srcDFrame, tableId, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        # table = client.get_table(table_id)  # Make an API request.
        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )

    def fetchDataFromGS(self, srcCSVPath, gsUrl, wsName):
        """ To fetch data from google sheet updated manually"""
        gc = gspread.service_account()
        workSheet = gc.open(gsUrl).worksheet(wsName)
        dFrame = pandas.DataFrame(workSheet.get_all_records())
        dFrame.to_csv(srcCSVPath)
        return True

class CMSTransfer( DataTransfer ):

    def __init__(self, cobDate, tableId, **kwargs):
        super(CMSTransfer, self).__init__( cobDate, tableId )
        assert self.checkInputs(kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

    def checkInputs(self, **kwargs):
        pass

    def fetchDataFromSource( self, taskName ):
        """To fetch CMS data from MongoDB or AWS Document DB"""
        return BashOperator(task_id=taskName, bash_command='bash /opt/airflow/bashScripts/dumpCMSMongoData.sh ' )

    def loadJsonToBigquery(self, srcJsonPath ):
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
        srcJsonPath = '/'.join([BaseDailyDumpPath, self.kwargs['mongoDumpPath']])
        return PythonOperator(task_id=taskName, python_callable = self.loadJsonToBigquery, op_kwargs={ 'srcJsonPath': srcJsonPath})    

class RSTTransfer(CMSTransfer):
    def __init__(self, cobDate, tableId, **kwargs):
        super(CMSTransfer, self).__init__( cobDate )
        assert self.checkInputs(kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

    def loadJsonToBigquery(self, srcJsonPath ):
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
        srcJsonPath = '/'.join([BaseDailyDumpPath, self.kwargs['mongoDumpPath']])
        return PythonOperator(task_id=taskName, python_callable = self.loadJsonToBigquery, op_kwargs={ 'srcJsonPath': srcJsonPath})

class CallerTransfer( DataTransfer ):
    def __init__(self, cobDate, tableId, **kwargs):
        super(CallerTransfer, self).__init__( cobDate )
        assert self.checkInputs(kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

    def fetchDataFromCSV(self, srcCSVPath):
        """ To fetch data from Exotel"""
        #Temporarily getting it from csv dump
        # Probably needs to be some logic to fetch from Exotel API 
        return pandas.read_csv(srcCSVPath)

    def fetchDataFromSource( self, taskName ):
        """To fetch Caller data from Exotel"""        
        csvDumpPath = '/'.join([BaseDailyDumpPath, self.kwargs['csvDumpPath']])
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromCSV, op_kwargs = { 'srcCSVPath' : csvDumpPath} )

    def loadDataToBigquery(self, taskName):
        """To load Caller data to Bigquery"""
        csvDumpPath = '/'.join([BaseDailyDumpPath, self.kwargs['csvDumpPath']])
        return PythonOperator(task_id=taskName, python_callable = self.loadCSVToBigquery, op_kwargs = { 'srcCSVPath' : csvDumpPath} )

class AssigneeTransfer( CallerTransfer ):
    def __init__(self, cobDate, tableId, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, tableId )
        assert self.checkInputs(kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs    

    def fetchDataFromSource( self, taskName ):
        """To fetch Caller data from Exotel"""
        csvDumpPath = '/'.join([BaseDailyDumpPath, self.kwargs['csvDumpPath']])
        opKwargs = {'srcCSVPath' : csvDumpPath, 'gsUrl': self.kwargs['gsUrl'], 'wsName': self.kwargs['sheetName']}
        return PythonOperator(task_id=taskName, python_callable = self.fetchDataFromGS, op_kwargs = opKwargs )

    # def loadDataToBigquery(self):
    #     """To load Caller data to Bigquery"""
    #     taskName = self.kwargs['loadTaskName']
    #     csvDumpPath = self.kwargs['csvDumpPath']
    #     return PythonOperator(task_id=taskName, python_callable = self.loadCSVToBigquery, op_kwargs = { 'srcCSVPath' : csvDumpPath} )

class CallStatusTransfer(AssigneeTransfer):
    def __init__(self, cobDate, tableId, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, tableId )
        assert self.checkInputs(kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

class OfficialsTransfer(AssigneeTransfer):
    def __init__(self, cobDate, tableId, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, tableId )
        assert self.checkInputs(kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

class StatusTransfer(AssigneeTransfer):
    def __init__(self, cobDate, tableId, **kwargs):
        super(AssigneeTransfer, self).__init__( cobDate, tableId )
        assert self.checkInputs(kwargs), 'Please provide inputs relevant for CMS data'
        self.kwargs = kwargs

def fetchTablefromBq(*args, **kwargs):
    pass

def loadTableDatatoGS(*args, **kwargs):
    pass        