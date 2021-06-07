import boto3
import re
from pyspark.sql import SparkSession
from io import BytesIO
from warcio.archiveiterator import ArchiveIterator

# code inspired by:
# https://netpreserve.org/ga2019/wp-content/uploads/2019/07/IIPCWAC2019-SEBASTIAN_NAGEL-Accessing_WARC_files_via_SQL-poster.pdf

bucket_name = 'maria-batch-1005'
bucket_index_key = 'athena/outputCSV/'
body_re = re.compile(rb"<body>(.*)</body>", re.DOTALL | re.IGNORECASE)

def get_warc_recs(sparkSession):
	"""yields a warc index file from S3 as a list of dicts
	containing pointers to the actual warc files"""
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(bucket_name)

	for obj in bucket.objects.filter(Delimiter='/', Prefix=bucket_index_key):
		if obj.key.endswith(".csv"):
			print(obj.key)
			print("s3a://" + bucket_name + "/" + obj.key)

			sqldf = sparkSession.read.format("csv").option("header",True) \
				.option("inferSchema",True).load("s3a://" + bucket_name + "/" + obj.key)
			yield sqldf.select("warc_filename","warc_record_offset","warc_record_length").rdd

def fetch_process_warc_records(rows):
	"""Fetch all WARC records as defined by each row in `rows` and process them"""
	s3client = boto3.client('s3')
	print("client gotten")
	for row in rows:
		warc_path = row['warc_filename']
		offset = int(row['warc_record_offset'])
		length = int(row['warc_record_length'])
		rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
		response = s3client.get_object(Bucket='commoncrawl',\
				Key=warc_path, Range=rangereq)
		record_stream = BytesIO(response["Body"].read())

		for record in ArchiveIterator(record_stream):
			page = record.content_stream().read()

			# HERE IS WHERE YOUR PROCESSING LOGIC GOES
			match_object = body_re.search(page)
			if(match_object):
				yield match_object.group(1)
			# do nothing if failure

if __name__ == '__main__':
	print("starting...")
	sparkSession = SparkSession.builder.appName('RevatureProject3').getOrCreate()
	try:
		for warc_recs in get_warc_recs(sparkSession):
			for i in warc_recs.mapPartitions(fetch_process_warc_records).collect():
				# post-processing, like saving to file or printing to screen
				print("\n\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n\n")
				print(i)
	finally:
		sparkSession.stop()
	print("done!")