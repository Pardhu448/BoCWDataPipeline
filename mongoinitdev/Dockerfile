FROM mongo:5

COPY submissions_dev_data.json /submissions_dev_data.json
CMD mongoimport --host bocwmongo --db devMongoDB --collection devMongoCollection --type json --file /submissions_dev_data.json --jsonArray
