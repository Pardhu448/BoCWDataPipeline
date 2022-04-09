from collections import namedtuple

# named tuple to define dataTransfer configuration
dataTransferConfig = namedtuple('DataTransferConfig', 'className inputArgs')

BaseDailyDumpPath = '/opt/airflow/data/dailyDataDump'

BqProjectLocation = 'asia-south1'

BqCentralStorageDataSet = 'BoCW'

BqOfficialsViewDataSet = 'GovtOfficials'
BqIAAssociatesViewDataSet = 'IAAssocisates'

districtDataFields = ['Date','Name', 'Contact_number', 'Profile', 'Sex','District','Area','Labor_card','Labor_card_number','Registration_date','Renewal_Date',	'People_in_need', 'Type_of_grievance','Details_grievance','Assigned_official','Date_assigned']
grievanceStatusDataFields = districtDataFields + ['Status', 'Date_resolved']

# named tuple to define data view configuration
# viewName: Custom name given to this view of data 
# sourceData: source data from which to fetch this data (tableID for Bq) 
# columnsRequired: Columns required out of the total columns available
# dateRange: date range to filter out required data ex: (20220402, 20220502) or 'PreviousDay' or 'PreviousWeek' or 'PreviousMonth' or 'PreviousYear'
# rowLimit: max number of rows to fetch
# destDataSet : new data set name to be created for this view 
dataViewConfig = namedtuple('dataViewConfig', 'viewName sourceDataTable destDataSet columnsRequired dateRange rowLimit')

#named tuple to define information required for a dag
# TaskName: unique sensible name for the task at hand(this goes as a dag ID in airflow)
# Schedule: Frequency at which dag needs to be triggered #Daily, Weekly, Monthly, Yearly, Once
# DataViewConfig: specification ofr the data view
DagInfo = namedtuple('DagInfo', 'TaskName Schedule DataViewConfig')