import re
import sys
import datetime
# configure spark variables
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

# load up other dependencies


manager_log = sys.argv[1]
application_log = sys.argv[2]
start = sys.argv[3]
end = sys.argv[4]

# lines = sc.textFile(manager_log)
# app_lines = sc.textFile(application_log)
#
# app_ids_filter = app_lines.filter(lambda x: re.search(r'1580812675067_\d+', x))\
#                  .map(lambda x: re.search(r'1580812675067_\d+', x).group(0))\
#                  .filter(lambda x: (int(re.search(r'1580812675067_(\d+)', x).group(1)) <= int(end)) & (int(re.search(r'1580812675067_(\d+)', x).group(1)) >= int(start)))
#
# app_ids_filter_list = app_ids_filter.distinct().collect()
#
# regex_ids_filter = re.compile("|".join(re.escape(app_id) for app_id in app_ids_filter_list))
#
# filtered_lines = lines.filter(lambda x: regex_ids_filter.search(x)).cache()
#
#
# appID_users = filtered_lines.filter(lambda x: re.search(r"Accepted application application_1580812675067_\d+ from user: \w+,", x))\
#                 .map(lambda x: re.search(r"Accepted application application_1580812675067_(\d+) from user: (\w+),", x).groups())\
#                 .map(lambda x: (int(x[0]), x[1]))
#
# appID_attemptID = filtered_lines.filter(lambda x: re.search(r'(Updating application attempt appattempt_1580812675067_\d+_\d+ with final state: FAILED)|(Updating application attempt appattempt_1580812675067_\d+_\d+ with final state: [A-Z]+, and exit status: [1-9]\d*)', x))\
#                            .map(lambda x: re.search(r'appattempt_1580812675067_(\d+)_(\d+)', x).groups())\
#                            .map(lambda x: (int(x[0]), int(x[1]))).sortByKey()
#
#
# appID_attemptID_startTime = filtered_lines.filter(lambda x: re.search(r'appattempt_1580812675067_\d+_\d+ State change from \w+ to LAUNCHED', x))\
#                              .map(lambda x: (re.search(r'appattempt_\d+_(\d+)_(\d+)', x).groups(),
#                                              datetime.datetime.strptime(re.search(r'^.{23}', x).group(0), '%Y-%m-%d %H:%M:%S,%f')))\
#                                             .map(lambda x: ((int(x[0][0]), int(x[0][1])), x[1]))\
#                              .sortByKey().cache()
#
# appID_attemptID_endTime = filtered_lines.filter(lambda x: re.search(r'appattempt_1580812675067_\d+_\d+ State change from FINAL_SAVING to (FINISHING|KILLED|FAILED)', x))\
#                              .map(lambda x: (re.search(r'appattempt_\d+_(\d+)_(\d+)', x).groups(),
#                                              datetime.datetime.strptime(re.search(r'^.{23}', x).group(0), '%Y-%m-%d %H:%M:%S,%f')))\
#                                             .map(lambda x: ((int(x[0][0]), int(x[0][1])), x[1]))\
#                              .sortByKey().cache()
#
# containers = filtered_lines.filter(lambda x: re.search("Assigned container container_e02_1580812675067_\d+_\d+_\d+ .* on host",x))\
#                             .map(lambda x: re.search("1580812675067_(\d+)_(\d+)_(\d+) .* on host (\w+.iccluster.epfl.ch)", x).groups())\
#                             .map(lambda x: ((int(x[0]), int(x[1])), ("container_e02_1580812675067_"+ x[0] + "_" + x[1][-2:] + "_" + x[2], x[3])))\
#                             .groupByKey().map(lambda x : (x[0], list(x[1])))
#
# appID_attemptID_users = appID_users.join(appID_attemptID).map(lambda x: ((x[0], x[1][1]), x[1][0])).cache()
#
# full_join = appID_attemptID_users.join(appID_attemptID_startTime).join(appID_attemptID_endTime).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1]))).join(containers).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1]))).cache()


sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter","***********************************************************************")
app_lines = sc.textFile(application_log)
containers = app_lines.filter(lambda x: (re.search(r'Container: container_e02_1580812675067_\d*_\d*_\d*', x))).map(lambda x: (re.search(r'Container: container_e02_1580812675067_(\d*_\d*_\d*)', x).group(1), x)).cache()
contID_task_stage = containers.filter(lambda x: re.search(r'[^ ]* at App[0-9*].scala:[0-9*][0-9*]\) failed .* task ([0-9*].[0-9*]) in stage ([0-9*].[0-9*])',x[1])).map(lambda x: (x[0], re.search(r'[^ ]* at App[0-9*].scala:[0-9*][0-9*]\) failed .* task ([0-9*].[0-9*]) in stage ([0-9*].[0-9*])',x[1]).groups())).cache()

x = contID_task_stage.collect()

output = [",".join(map(str, item)) for item in x]
text_file = open("Output.txt", "w")
text_file.write(str(output))
text_file.close()
