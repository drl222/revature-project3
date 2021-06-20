package project3

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.spark.sql.functions.{col, date_format, lower, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.zip.GZIPInputStream
import org.apache.spark.storage.StorageLevel.DISK_ONLY


object JobTrends {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Job Posting Trends")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // Utility Constants
    val saveToBucket = "maria-batch-1005"
    val saveToKey = "/gfkContribution/outputContent/JobTrendsData" // S3 Directory to save Output to
    val genSrc = "s3a://commoncrawl/cc-index/table/cc-main/warc/" // WARC S3 Bucket Directory
    val jobDomainList = List[String]("indeed.com") //list of websites we want to scrape

    val warcSetYear = List[String](
      "CC-MAIN-2020-05",
      "CC-MAIN-2020-06",
      "CC-MAIN-2020-07",
      "CC-MAIN-2020-08",
      "CC-MAIN-2020-09",
      "CC-MAIN-2020-10",
      "CC-MAIN-2020-11",
      "CC-MAIN-2020-12",
      "CC-MAIN-2021-01",
      "CC-MAIN-2021-02",
      "CC-MAIN-2021-03",
      "CC-MAIN-2021-04",
      "CC-MAIN-2021-05"
    )

    //Regex Strings
    val contentBlock = "<div class=\\\"job-openings__job\\\">"
    val contentRegex = ".*<h2 class=\"jobcard__title\">([A-Za-z0-9\\s,]+)</h2>.*<div class=\"jobcard__subtitle\">([A-Za-z0-9\\s,]+)</div>.*<div class=\"jobcard__indicator\">([A-Za-z0-9\\s,]+)</div>.*".r

    try {
      //'try' to execute the data reads ...
      dataMay21(1000000) //so far, it is fine
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
    def dataMay21(siteCap: Int): Unit = {
      // 'genSrc' is this:    "s3a://commoncrawl/cc-index/table/cc-main/warc/"
      val dataDF = spark.read
        .option("inferSchema", "true")
        .option("header", value = true)
        .load(genSrc) //load in the common crawl from bucket
      val dataTrim = dataDF
        .filter($"subset" === "warc") //get the warc file
        .filter($"fetch_status" === 200) // Filter on HTTP status code 'success'
        .filter($"crawl".isin(warcSetYear: _*))
        .filter($"url_host_registered_domain".isin(jobDomainList: _*)) //Taken from Fransisco, to search for Indeed
        .filter(lower($"url_path").contains("job")) // Filter on full URL path contains the word 'job'
        .select($"warc_filename", $"warc_record_offset", $"warc_record_length", date_format($"fetch_time", "yyyyMMdd").as("warc_time"))
        .filter(lower($"url_path").contains("programmer") ||
          lower($"url_path").contains("engineer") ||
          lower($"url_path").contains("software") ||
          lower($"url_path").contains("computer") ||
          lower($"url_path").contains("developer") ||
          lower($"url_path").contains("tech support") ||
          lower($"url_path").contains("network") ||
          lower($"url_path").contains("analyst") ||
          lower($"url_path").contains("big data") ||
          lower($"url_path").contains("it specialist") ||
          lower($"url_path").contains("technician") ||
          lower($"url_path").contains("information technology") ||
          lower($"url_path").contains("full stack") ||
          lower($"url_path").contains("front end") ||
          lower($"url_path").contains("back end"))
        .limit(siteCap)

      dataTrim.toDF().coalesce(8).write.mode("overwrite").save("s3a://" + saveToBucket + saveToKey + "/Source").persist(DISK_ONLY)
      val dataUse = dataTrim.collect

      //AWS Content
      val s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(Regions.US_EAST_1)
        .build

      val coreContent: DataFrame = spark.sparkContext.parallelize(Seq("Job_Title::Company::Location::WARC_Access_Timestamp_as_String")).toDF("HTML FUll")

      dataUse.foreach(x => {
        val warcFileP = x(0).toString
        val sectionOS = x(1).asInstanceOf[Int]
        val sectionLN = x(2).asInstanceOf[Int]
        val timeString = x(3).toString
        val request: GetObjectRequest = new GetObjectRequest("commoncrawl", warcFileP).withRange(sectionOS, sectionOS + sectionLN - 1)
        val s3Object = scala.io.Source.fromInputStream(new GZIPInputStream(s3Client.getObject(request).getObjectContent))
        val s3Content = s3Object.mkString
        val contentCatcher = udf((htmlContent: String) => if (htmlContent.startsWith("<a")) contentRegex.unapplySeq(htmlContent).getOrElse(List(null)).mkString("::") else null)
        val addTime = udf((colTarget: String) => colTarget + s"::$timeString")
        val dfToWrite = spark.sparkContext.parallelize(Seq(s3Content.substring(s3Content.indexOf("<!DOCTYPE"), s3Content.length)))
          .map(a => Some(a.slice(a.indexOf("<div class=\"job-openings__list\">"), a.indexOf("<a target=\"_blank\""))).orNull)
          .filter(_ != "")
        if (!dfToWrite.isEmpty()) {
          val dfFinal = dfToWrite.flatMap(_.split(contentBlock))
            .map(_.replace("\\\"", "\"").replace("\\\\\"", "\"").replace("\n", "").trim)
            .toDF("HTML Content")
            .withColumn("HTML Extract", contentCatcher(col("HTML Content")))
            .withColumn("HTML Full", addTime($"HTML Extract"))
            .filter(!col("HTML Extract").like("null"))
            .drop($"HTML Content").drop($"HTML Extract").na.drop
          coreContent.union(dfFinal)
        }
        s3Object.close
      })
      coreContent.coalesce(8).write.format("csv").mode("append").save("s3a://" + saveToBucket + saveToKey + "/Extract")
      coreContent.unpersist()
    }
  }
}
