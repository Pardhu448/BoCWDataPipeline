FROM mongo:5

COPY rst_dev_data.json /rst_dev_data.json

COPY cms_dev_data.json /cms_dev_data.json

CMD mongoimport --host bocwmongo --db devMongoDB --collection CMSDevData --type json --file /cms_dev_data.json;mongoimport --host bocwmongo --db devMongoDB --collection RSTDevData --type json --file /rst_dev_data.json --jsonArray --mode upsert

#CMD ls *.json | sed 's/.json//' | while read col; do mongoimport --host bocwmongo -d devMongoDB -c $col --file $col.json --type json --jsonArray; done
