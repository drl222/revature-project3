import json
import csv
from pyspark.sql import SparkSession

# with open('processed_job_listings.json', 'r', encoding='utf-8') as f:
# 	final_table = json.load(f)

if __name__ == '__main__':
	print("starting...")
	sparkSession = SparkSession.builder.appName('RevatureProject3').getOrCreate()
	print()
	try:
		sqldf = sparkSession.read.format("json").load("processed_job_listings2.json")
		sqldf.show()
		sqldf.coalesce(1).write.format('json').save('processed_job_listings4.json')
	finally:
		sparkSession.stop()
	print("done!")