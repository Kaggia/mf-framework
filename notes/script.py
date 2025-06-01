import json
import dateutil
import datetime
import dateutil.parser

in_data = json.load(open('data_wherug_.json', "r+"))['data']
for i, _ in enumerate(in_data):
    in_data[i]['sampleDate'] = in_data[i]['sampleDate'].split("/")[2] + "-" + str(int(in_data[i]['sampleDate'].split("/")[1])).zfill(2) + "-" + str(int(in_data[i]['sampleDate'].split("/")[0])).zfill(2) + " 00:00"

json.dump({'data':in_data}, open('data_wherug.json', "w+"), indent=4)