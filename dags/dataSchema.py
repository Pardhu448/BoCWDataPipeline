# File with data schema formats for Data Tables in BigQuery
from google.cloud import bigquery
from constants import districtDataFields, grievanceStatusDataFields

#Making them all string for now
#ToDo: {parthaE;Abhishek;Ananya} need to provide right schema based on actual data   
districtsSchema = [ bigquery.SchemaField(eaField, "STRING") for eaField in districtDataFields ]
grievanceStatusSchema =[ bigquery.SchemaField(eaField, "STRING") for eaField in grievanceStatusDataFields]





