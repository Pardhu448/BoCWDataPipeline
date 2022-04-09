import os 

def fetchRefinedTableID(storageProject, dataSet, tableId):
    '''Create refined table name from project name, dataset and tableID'''
    return '.'.join([storageProject, dataSet, tableId])

def fetchRefinedDataSetID(storageProject, dataSet):
    '''Create refined table name from project name, dataset and tableID'''
    return '.'.join([storageProject, dataSet])    