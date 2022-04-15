# Configuration file to specify different data views required for BoCW project
# Each view is associated with the corresponding stakeholders interested in this view of raw data

from collections import namedtuple
from utils import fetchRefinedTableID, fetchRefinedDataSetID
from constants import dataViewConfig, DagInfo
import os 

class BoCWDataViewConfigs:
    #All configurations required for different views on source data are stored here
    #In case new data view is required, please update it as an attribute in this class
    def dataViewConfig(self):
        raise NotImplementedError("Subclasses should implement this!")

    def __init__(self, configName, **kwargs):
        self.configName = configName

    def getSchedule(self):
        '''To get schedule in the format required by Airflow'''
        return None

class dailyCMSAppData(BoCWDataViewConfigs):
    ## To fetch daily data requied by IA associates
    ### Beneficiary data from CMS app
    def dataViewConfig(self):
        
        StorageProject = os.environ.get('CentralStorageProject')
        SrcDataTable = fetchRefinedTableID(StorageProject, 'BoCW', 'CMS')
        
        ViewProject = os.environ.get('BqCurrentProject')
        DestDataSet = fetchRefinedDataSetID(ViewProject, 'IAAssociates')
        #Columns required for data datafiltering
        ColsRequired = ['data.persistent_data_district', 'data.identification_assigneeName', 'document_name', 'mobile', 'operation', 'state', 'created_at']
        
        viewTableName = 'CMSFiltered'
        gsWorksheet = 'https://docs.google.com/spreadsheets/d/1t9Wg-w72v-daCdliPuayg8BWPh7G-l-aK6oJEnhAq4M/edit#gid=0'
        return dataViewConfig(self.configName, SrcDataTable, DestDataSet, viewTableName, gsWorksheet, ColsRequired, 'PreviousDay', 1000)  

class dailyExotelAppData(BoCWDataViewConfigs):
    ###Caller data from Exotel
    def dataViewConfig(self):
        
        StorageProject = os.environ.get('CentralStorageProject')
        SrcDataTable = fetchRefinedTableID(StorageProject, 'BoCW', 'Caller')

        ViewProject = os.environ.get('BqCurrentProject')
        DestDataSet = fetchRefinedDataSetID(ViewProject, 'IAAssociates')

        ColsRequired = ['UniqueID_Exotel','PhoneNumber_Beneficiary','DateTime','Count']
        
        viewTableName = 'CallerFiltered'
        gsWorksheet = 'https://docs.google.com/spreadsheets/d/1NHipt4yGW8yVMyeBiJpyTFzDtpA7bs4SWfaf4-uvPgk/edit#gid=0'
        return dataViewConfig(self.configName, SrcDataTable, DestDataSet, viewTableName, gsWorksheet, ColsRequired, 'PreviousDay', 1000)

class lastYearGrievanceMetrics(BoCWDataViewConfigs):
    ## Smaple Adhoc view of central sorage Table
    ### To fetch last year data of grievance status for adhoc analysis by IA associates
    def dataViewConfig(self):
        StorageProject = os.environ.get('CentralStorageProject')
        SrcDataTable = fetchRefinedTableID(StorageProject, 'BoCW', 'GrievanceStatus')

        ViewProject = os.environ.get('BqCurrentProject')
        DestDataSet = fetchRefinedDataSetID(ViewProject, 'IAAssociates')

        ColsRequired = ['data.persistent_data_district', 'data.identification_assigneeName', 'document_name', 'mobile', 'operation', 'state', 'created_at', 'Type_of_grievance','Details_grievance', 'Status', 'Date_resolved']
        
        viewTableName = 'LastYearGrievanceMetrics'
        gsWorksheet = 'https://docs.google.com/spreadsheets/d/1nL83t7KKxRZexHe8bN5W3QoSvWIPmK9yHQkH3FPiNQA/edit#gid=0'
        return dataViewConfig(self.configName, SrcDataTable, DestDataSet, viewTableName, gsWorksheet, ColsRequired, 'PreviousDay', 1000)

def DataViewDagConfigMap(env=None):
    """To load all data configs required for each dag associated with a particular data view to be made available through ariflow """
    #Please add config here for any new Data View Task
    return { 
        'EOD_BeneficiaryData' : DagInfo('EOD_BeneficiaryData', 'Once', dailyCMSAppData('EOD_BeneficiaryData').dataViewConfig()),
        'EOD_CallerData': DagInfo('EOD_CallerData', 'Once', dailyExotelAppData('EOD_CallerData').dataViewConfig()),
        'ADHOC_LastYearGrievanceMetrics': DagInfo('ADHOC_LastYearGrievanceMetrics', 'Once', lastYearGrievanceMetrics('ADHOC_LastYearGrievanceMetrics').dataViewConfig())
         }
