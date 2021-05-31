#!/usr/bin/env python3

import isilon_helper
from datetime import datetime
import sys
from elasticsearch import helpers
from es_connect import connect_elasticsearch

if len(sys.argv) >= 2:
    host = sys.argv[1]
else:
    sys.exit("Missing Isilon Host Information.  Exiting program.")

cluster = sys.argv[1]
#cluster = 'cluster1'

job_list_url = f'https://{cluster}:8080/platform/3/sync/jobs'
report_list_url = f'https://{cluster}:8080/platform/4/sync/reports'

#job_list = isilon_helper.get_metric(job_list_url)
report_list = isilon_helper.get_metric(report_list_url)

#collection_date = datetime.now().strftime('%Y%m%d%H%M%S')
collection_date = datetime.now().strftime('%s')

report_data = report_list['reports']
for item in report_data:
    if item['error']:
        success = False
    else:
        success = True

    item.update({"timestamp": collection_date, "cluster": cluster, "success": success})

"""
filename = "repo/isilon_sycnip_" + cluster + "_" + collection_date
with open(filename, 'a') as output:
    output.write('collection_date, cluster\n')
    output.write(f'{collection_date}, {cluster}\n')
    output.write(f'\n')
    output.write(f'{report_data}\n')
print(filename)
"""

es = connect_elasticsearch()

resp = helpers.bulk(es, report_data, index='isilon_synciq', doc_type='_doc')
