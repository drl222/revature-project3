package project3

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.util.zip.GZIPInputStream
class JobTrends {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Job Posting Trends")
      .config("fs.s3a.access.key", "AKIA4OK5FKIY5TZ3JJ7I")
      .config("fs.s3a.secret.key", "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")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // Utility Constants
    //val saveToBucket = "s3a://maria-batch-1005/gfkContribution/outputContent/" // S3 Directory to save Output to
    val genSrc = "s3a://commoncrawl/cc-index/table/cc-main/warc/" // WARC S3 Bucket Directory
    val sampleWARC = "CC-MAIN-2020-05"
    val outputSchema = StructType(StructField("Source URL", StringType, nullable = false) :: StructField("HTML Content", StringType, nullable = true) :: Nil)

    try {
      //'try' to execute the data reads ...
      dataMay21(sampleWARC, 1) //so far, it is fine
    }
    catch {
      // ... 'catch' any mistakes ...
      case e: Exception => println("Oops: " + e.getMessage)
    }
    finally {
      // ... end the spark session, no matter what
      spark.close()
    }


    // Read data for job trends for the last month of May 2021
    def dataMay21(crawl: String, siteCap: Int): Unit = {
      // 'crawl' is this:   "CC-MAIN-2021-05"
      // 'genSrc' is this:    "s3a://commoncrawl/cc-index/table/cc-main/warc/"
      val dataDF = spark.read.option("inferSchema", "true").option("header", value = true).load(genSrc) //load in the common crawl from bucket
      val dataTrim = dataDF.select($"url", $"warc_filename", $"warc_record_offset", $"warc_record_length") //get (1) site source, (2) warc file source
        .filter($"subset" === "warc") //get the warc file
        .filter($"crawl" === crawl) //get the particular crawl from the common
        .filter($"fetch_status" === 200) // Filter on HTTP status code 'success'
        .filter($"url_path".contains("job")) // Filter on full URL path containing the word 'job'
        .filter($"url_host_name".contains("indeed") || $"url_host_name".contains("glassdoor")) //Filter fo sites Indeed and Glassdoor
        .limit(siteCap)
        .collect

      //AWS Content
      val s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(Regions.US_EAST_1)
        .build

      dataTrim.foreach(x => {
        val entityURL = x(0).toString
        val warcFileP = x(1).toString
        val sectionOS = x(2).asInstanceOf[Int]
        val sectionLN = x(3).asInstanceOf[Int]
        val request: GetObjectRequest = new GetObjectRequest("commoncrawl", warcFileP).withRange(sectionOS, sectionOS + sectionLN - 1)
        val s3Object = scala.io.Source.fromInputStream(new GZIPInputStream(s3Client.getObject(request).getObjectContent))
        val s3Content = s3Object.mkString
        val dfToWrite: DataFrame = spark.sparkContext.parallelize(Seq(s3Content.substring(s3Content.indexOf("<!DOCTYPE"), s3Content.length))).toDF("HTML Content")
        //dfToWrite.write.mode("append").csv(saveToBucket) // need this to write the final row entities to file
        dfToWrite.show(false)
        s3Object.close
      })

    }
  }

}
