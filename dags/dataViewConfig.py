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
        
        ColsRequired = ['Date','Name', 'Contact_number', 'Profile', 'Sex','District','Area','Labor_card','Labor_card_number','Registration_date','Renewal_Date',	'People_in_need', 'Type_of_grievance','Details_grievance']
        
        viewTableName = 'CMSFiltered'
        return dataViewConfig(self.configName, SrcDataTable, DestDataSet, viewTableName, ColsRequired, 'PreviousDay', 1000)  

class dailyExotelAppData(BoCWDataViewConfigs):
    ###Caller data from Exotel
    def dataViewConfig(self):
        
        StorageProject = os.environ.get('CentralStorageProject')
        SrcDataTable = fetchRefinedTableID(StorageProject, 'BoCW', 'Caller')

        ViewProject = os.environ.get('BqCurrentProject')
        DestDataSet = fetchRefinedDataSetID(ViewProject, 'IAAssociates')

        ColsRequired = ['UniqueID_Exotel','PhoneNumber_Beneficiary','DateTime','Count']
        
        viewTableName = 'CallerFiltered'
        return dataViewConfig(self.configName, SrcDataTable, DestDataSet, viewTableName, ColsRequired, 'PreviousDay', 1000)

class lastYearGrievanceMetrics(BoCWDataViewConfigs):
    ## Smaple Adhoc view of central sorage Table
    ### To fetch last year data of grievance status for adhoc analysis by IA associates
    def dataViewConfig(self):
        StorageProject = os.environ.get('CentralStorageProject')
        SrcDataTable = fetchRefinedTableID(StorageProject, 'BoCW', 'GrievanceStatus')

        ViewProject = os.environ.get('BqCurrentProject')
        DestDataSet = fetchRefinedDataSetID(ViewProject, 'IAAssociates')

        ColsRequired = ['Date','Name', 'Contact_number', 'Profile', 'Sex','District','Area','Labor_card','Labor_card_number','Registration_date','Renewal_Date',	'People_in_need', 'Type_of_grievance','Details_grievance', 'Status', 'Date_resolved']
        
        viewTableName = 'LastYearGrievanceMetrics'
        return dataViewConfig(self.configName, SrcDataTable, DestDataSet, viewTableName, ColsRequired, 'PreviousDay', 1000)

def DagConfigMap(env=None):
    """To load all data configs required for each dag associated with a particular data view to be made available through ariflow """
    #Please add config here for any new Data View Task
    return { 
        'EOD_BeneficiaryData' : DagInfo('EOD_BeneficiaryData', 'Daily', dailyCMSAppData('EOD_BeneficiaryData')),
        'EOD_CallerData': DagInfo('EOD_CallerData', 'Daily', dailyExotelAppData('EOD_CallerData')),
        'ADHOC_LastYearGrievanceMetrics': DagInfo('ADHOC_LastYearGrievanceMetrics', 'Once', lastYearGrievanceMetrics('ADHOC_LastYearGrievanceMetrics'))
         }
