
#All queries for analysis on data on BigQuery are available here 

def queryDateFilteredData(srcDataTableId, columnFilter, dateRange):
    formattedCols = ', '.join(columnFilter)
    if dateRange == 'PreviousDay':
        return """SELECT {} FROM {} WHERE EXTRACT(DATE FROM created_at AT TIME ZONE "UTC") < DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) LIMIT 100""".format(formattedCols, srcDataTableId)