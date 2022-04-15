import os 

def fetchRefinedTableID(storageProject, dataSet, tableId):
    '''Create refined table name from project name, dataset and tableID'''
    return '.'.join([storageProject, dataSet, tableId])

def fetchRefinedDataSetID(storageProject, dataSet):
    '''Create refined table name from project name, dataset and tableID'''
    return '.'.join([storageProject, dataSet])

def getScheduleInterval(schedule, triggerTime='0 2 * * *'):
    '''To get schedule in the format required in Airflow'''
    if schedule == 'Daily':
        return triggerTime
    elif schedule == 'Once':
        return '@once'

from google.cloud.exceptions import NotFound
def checkIfDatasetExistsInBq(client, refinedDatasetId):
    try:
        client.get_dataset(refinedDatasetId)  # Make an API request.
        print("Dataset {} already exists".format(refinedDatasetId))
    except NotFound:
        print("Dataset {} is not found".format(refinedDatasetId))
        return False
    return True                