#! /bin/bash 

#DIR=$(cd "$( dirname "${BASH_SOURCE[0]}") " && pwd )
#source "$DIR/../.env"
#set timeStamp = $(TZ=IST date +"%Y-%m-%d %T")

mongoexport --host bocwmongo:27017 --db devMongoDB --collection RSTDevData --type json --out "/opt/airflow/data/dailyDataDump/rstDataSnapShot.json" --jsonArray
