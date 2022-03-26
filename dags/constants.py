from collections import namedtuple

# named tuple to define dataTransfer configuration
dataTransferConfig = namedtuple('DataTransferConfig', 'className inputArgs')

BaseDailyDumpPath = '/opt/airflow/data/dailyDataDump'