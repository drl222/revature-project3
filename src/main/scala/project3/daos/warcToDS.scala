package project3.daos

import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object warcToDS {

  /** warcToDS()
   *
   *  Given an s3a://commoncrawl warc.gz path, this will output a Dataset of WARC files.
   *  Ideally, you received the warc.gz path by filtering or querying s3a://commoncrawl/cc-index/table/cc-main/warc/
   *  @input spark, the spark session currently in use.
   *  @input warcPath, a string of the warc.gz path
   *      ex. 's3a://commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084887973.50/warc/CC-MAIN-20180119105358-20180119125358-00406.warc.gz'
   *  @return Dataset[String], you can save it as a CSV or something.
   */
  def warcToDS (spark : SparkSession, warcPath : String) : Dataset[String] = {
    val testWarc = warcPath
    val testRead = spark.read.option("lineSep", "WARC/1.0").textFile(testWarc)
    testRead.printSchema()
    import spark.implicits._
    testRead
      .filter($"value".contains("jobs"))
      .filter(lower($"value").contains("engineer") ||
      lower($"value").contains("software") ||
      lower($"value").contains("computer") ||
      lower($"value").contains("developer") ||
      lower($"value").contains("java") ||
      lower($"value").contains("information technology") ||
      lower($"value").contains("it specialist") ||
      lower($"value").contains("tech support") ||
      lower($"value").contains("technical support") ||
      lower($"value").contains("network") ||
      lower($"value").contains("technician") ||
      lower($"value").contains("analyst"))
  }
}
