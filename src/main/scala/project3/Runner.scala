package project3

import keys.keys
import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession
  .builder()
  .master("local[*]")
  .appName("commoncrawl demo")
  .config("spark.hadoop.fs.s3a.access.key", keys.AccessKey)
  .config("spark.hadoop.fs.s3a.secret.key", keys.SecretKey)
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
  .config("spark.hadoop.fs.s3a.fast.upload","true")
  .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
  .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")
  val commonCrawlDF = spark.read
  .option("inferSchema", "true")
  .option("header", true)
  .load("s3a://commoncrawl/cc-index/table/cc-main/warc/")


  //What percent of tech job posters post no more than three job ads a month?
  Questions.QuestionFive.QuestionFive.questionFive(spark, commonCrawlDF)

  spark.close()
  }

}
