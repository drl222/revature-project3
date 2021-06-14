from pyspark import SparkContext
from operator import add
import time

bucket_name = 'maria-batch-1005'
bucket_output_key = 'Dylan/data/PythonScriptOutput/'

sc = SparkContext()  
data = sc.parallelize(list("Hello World"))  
counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).coalesce(1)

counts.saveAsTextFile("s3a://" + bucket_name + "/" + bucket_output_key + "/" + 'helloworld%d.json' % time.time())
sc.stop()