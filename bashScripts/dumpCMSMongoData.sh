#! /bin/bash 

#DIR=$(cd "$( dirname "${BASH_SOURCE[0]}") " && pwd )
#source "$DIR/../.env"
#set timeStamp = $(TZ=IST date +"%Y-%m-%d %T")
mongoexport --host bocwmongo:27017 --db devMongoDB --collection CMSDevData --type json --out "/opt/airflow/data/dailyDataDump/CMSfromMongoSnapShot.json"

