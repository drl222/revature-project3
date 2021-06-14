import boto3
import re
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql.functions import udf, explode
from io import BytesIO
from warcio.archiveiterator import ArchiveIterator

# code inspired by:
# https://netpreserve.org/ga2019/wp-content/uploads/2019/07/IIPCWAC2019-SEBASTIAN_NAGEL-Accessing_WARC_files_via_SQL-poster.pdf

bucket_name = 'maria-batch-1005'
# bucket_index_key = 'athena/outputCSV/'
bucket_index_key = 'Dylan/data/testinput/'
bucket_output_key = 'Dylan/data/PythonScriptOutput/'
search_re = re.compile(rb"<body>(.*)</body>", re.DOTALL | re.IGNORECASE)

company_re = re.compile(r'<h1 class="cmp-section-first-header">(.*?)</h1>')
entire_posting_re = re.compile(r'<li class="cmp-section cmp-job-entry">(.*?)</li>.+?')
jobtitle_re = re.compile(r'<h3><a class="cmp-job-url" href=".+?" target=".+?" rel=".+?">(.*?)</a></h3>')
location_re = re.compile(r'<div class="cmp-note">(.*?)</div>')
description_re = re.compile(r'<div class="cmp-job-snippet">(.*?)</div>')
time_re = re.compile(r'<div class="cmp-note cmp-relative-time">(.*?)</div>')

def get_warc_recs(sparkSession):
	"""yields a warc index file from S3 as a list of dicts
	containing pointers to the actual warc files"""
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(bucket_name)

	for obj in bucket.objects.filter(Delimiter='/', Prefix=bucket_index_key):
		if obj.key.endswith(".csv"):
			print("s3a://" + bucket_name + "/" + obj.key)

			sqldf = sparkSession.read.format("csv").option("header",True) \
				.option("inferSchema",True).load("s3a://" + bucket_name + "/" + obj.key)
			yield sqldf.select("warc_filename","warc_record_offset","warc_record_length")

def get_warc_recs_from_local(sparkSession, filename):
	sqldf = sparkSession.read.format("csv").option("header",True) \
		.option("inferSchema",True).load(filename)
	yield sqldf.select("warc_filename","warc_record_offset","warc_record_length")

# deprecated
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
			page = record.content_stream().read().decode('utf-8')

			# HERE IS WHERE YOUR PROCESSING LOGIC GOES
			# match_object = search_re.search(page)
			# if(match_object):
				# yield match_object.group(1)
			yield find_postings_in_page(page)
			# do nothing if failure

def fetch_and_process_one_warc_record(warc_path, offset, length):
	"""Fetch all WARC records as defined by each row in `rows` and process them"""
	s3client = boto3.client('s3') # getting this each time might be expensive
	rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
	response = s3client.get_object(Bucket='commoncrawl',\
			Key=warc_path, Range=rangereq)
	record_stream = BytesIO(response["Body"].read())

	for record in ArchiveIterator(record_stream):
		page = record.content_stream().read().decode('utf-8')
		# HERE IS WHERE YOUR PROCESSING LOGIC GOES

		# despite the for loop, exactly one should exist in the ArchiveIterator
		# by the nature of how the original index is
		# so just immediately return in the first iteration of the loop
		return find_postings_in_page(page)

def find_postings_in_page(fulltext):
	company = company_re.search(fulltext)
	list_to_return = []

	if company:
		company_name = company.group(1)

		for this_li in entire_posting_re.finditer(fulltext):
			try:
				entire_posting = this_li.group(1)
				jobtitle = jobtitle_re.search(entire_posting)
				description = description_re.search(entire_posting)
				location = location_re.search(entire_posting)
				time = time_re.search(entire_posting)

				list_to_return.append({
					"company": company_name,
					"jobtitle": jobtitle.group(1),
					"description": description.group(1),
					"location": location.group(1),
					"time": time.group(1)
					})
			except AttributeError:
				print("Error while matching regex")
	return list_to_return

if __name__ == '__main__':
	print("starting...")
	sparkSession = SparkSession.builder.appName('RevatureProject3').getOrCreate()
	print()
	try:
		my_udf = udf(fetch_and_process_one_warc_record, ArrayType(
			StructType([
				StructField("company", StringType()),
				StructField("jobtitle", StringType()),
				StructField("description", StringType()),
				StructField("location", StringType()),
				StructField("time", StringType())
				])
			))
		for warc_recs in get_warc_recs(sparkSession):
		# for warc_recs in get_warc_recs_from_local(sparkSession, "indeed_CCindex_20.csv"):
			final_table = warc_recs.select(
				explode(my_udf("warc_filename", "warc_record_offset", "warc_record_length")).alias("row"))\
				.select("row.*")

			final_table.show()
			final_table.write.format('json').save("s3a://" + bucket_name + "/" + bucket_output_key + "/" + 'processed_job_listings_%d.json' % time.time())
	finally:
		print("done")
		sparkSession.stop()