package project3


import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.GetObjectRequest

import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CodingErrorAction
import keys.keys
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.util.zip.GZIPInputStream


object Runner {
  final val RUN_ON_EMR = false;



    val s3Creds = new BasicAWSCredentials(keys.AccessKey, keys.SecretKey)
    val s3Client =
      AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(s3Creds))
        .withRegion("us-east-1")
        .build()

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
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

    /**
    val commonCrawlDF = spark.read
      .option("inferSchema", "true")
      .option("header", true)
      .load("s3a://commoncrawl/cc-index/table/cc-main/warc/")
    **/
    //s3a://maria-batch-1005/athena/

    //Amazon S3 Setup

    val s3OutputBucket = "s3a://franciscotest/"
    val crawl = "CC-MAIN-2021-21"
    val subset = "warc"

    /**
    //"crawl-data/CC-MAIN-2018-09/segments/1518891812584.40/warc/CC-MAIN-20180219111908-20180219131908-00636.warc.gz"
    val jobDomainList = List[String]("indeed.com")
    val filteredCommonCrawlDF = commonCrawlDF
      .filter( $"crawl" === crawl)
      .filter($"fetch_status" === 200)
      .filter($"subset" === subset)
      .filter($"content_languages".like("%eng%"))
      .filter($"url_host_registered_domain".isin(jobDomainList:_*))
      .filter($"url".like("%indeed.com/cmp/%/job%"))
      .filter(lower($"url_path").like("%job%"))
      .filter(lower($"url_path").contains("programmer") ||
        lower($"url_path").contains("engineer") ||
        lower($"url_path").contains("software") ||
        lower($"url_path").contains("computer") ||
        lower($"url_path").contains("developer") ||
        lower($"url_path").contains("java") ||
        lower($"url_path").contains("information technology") ||
        lower($"url_path").contains("it specialist") ||
        lower($"url_path").contains("tech support") ||
        lower($"url_path").contains("technical support") ||
        lower($"url_path").contains("network") ||
        lower($"url_path").contains("technician") ||
        lower($"url_path").contains("analyst")
      )
      .select("url","url_path","warc_filename", "warc_record_offset", "warc_record_length")
      .limit(5)
    **/

    //filteredCommonCrawlDF.show(2,false)
    val  customSchema = StructType(
      Seq(
        StructField("index", LongType, true),
        StructField("warc_filename", StringType, true),
        StructField("warc_record_offset", LongType, true),
        StructField("warc_record_length", LongType, true),
        StructField("timestamp", StringType, true),
        StructField("url", StringType, true)
      )
    )
    val filteredCommonCrawlDF =
      spark.read.format("csv")
        .option("delimiter", "\t")
        .schema(customSchema)
        .load("s3a://maria-batch-1005/athena/small-testing/indeed_sample.txt")

    filteredCommonCrawlDF.show(false)

    val warcFilesInfo =
      filteredCommonCrawlDF
        .select("warc_filename", "warc_record_offset", "warc_record_length")
        .map(row=> (row.getString(0), row.getLong(1), row.getLong(2)))

    val company_re = "<span class=\"company\">(.*?)</span>".r
    val jobtitle_re = "<b class=\"jobtitle\"><font size=\"\\+1\">(.*?)</font></b>".r
    val location_re = "<span class=\"location\">(.*?)</span>".r
    val description_re = "<span id=\"job_summary\" class=\"summary\">(.*?)</span>".r
    val time_re = "<span class=\"date\">(.*?)</span>".r
    println("starting nest")

    val dataRetrievalDF = warcFilesInfo.map(
      warcRow => {
        val warcContent: String = getContentFromS3Object(warcRow._1, warcRow._2, warcRow._3)
        println(" NEWWWWW  WARC CONTENT")
        //println(warcContent)

        var company = ""
        company_re.findFirstMatchIn(warcContent) match {
          case Some(value) => company = value.group(1)
          case None => {
            company = "N/A"
            println("No Company Found")
          }
        }

        var jobTitle = ""
        var description = ""
        var location = ""
        var time = ""


        jobtitle_re.findFirstMatchIn(warcContent) match {
          case Some(value) => jobTitle = value.group(1)
          case None => {
            jobTitle = "N/A"
            println("JobTitle Not Available")
          }
        }

        description_re.findFirstMatchIn(warcContent) match {
          case Some(value) => description = value.group(1)
          case None => {
            description = "N/A"
            println("Description Not Available")
          }
        }

        location_re.findFirstMatchIn(warcContent) match {
          case Some(value) => location = value.group(1)
          case None => {
            location = "N/A"
            println("Location Not Available")
          }
        }

        time_re.findFirstMatchIn(warcContent) match {
          case Some(value) => time = value.group(1)
          case None => {
            time = "N/A"
            println("Time Not Available")
          }
        }

        (company,jobTitle, description, location, time)
      }
    )
    dataRetrievalDF.show(false)
    spark.close()
  }

  /**
   * getContentFromS3Object utilizes the Amazon S3 Java API to specifically traverse the Common Crawl Index by
   * using the withRange function of S3Objects to skip and point to the correct Warc Page content (using file offset and length)
   * within the accumulated file (@param filename) that contains multiple warc pages.
   * @param filename = the filename path excluding the root bucket
   * @param fileOffset = the respective file offset for fileName
   * @param fileLength = the respective file length for fileName
   * @param s3Client = the Amazon S3 Client object containing credentials
   * @return = a String of the entire content of the fileName
   */
  def getContentFromS3Object(filename: String, fileOffset: Long, fileLength: Long): String = {
    //https://stackoverflow.com/questions/24537884/how-to-get-first-n-bytes-of-file-from-s3-url
    //https://stackoverflow.com/questions/13625024/how-to-read-a-text-file-with-mixed-encodings-in-scala-or-java

    //make a request to get the file from commoncrawl
    val request = new GetObjectRequest("commoncrawl",filename)
    //set the specific page within the warc file
    request.withRange(fileOffset, fileOffset + fileLength - 1)
    //retrieve it
    val s3WarcObject = s3Client.getObject(request)
    //check for any malformed input in the input stream
    val decoder = Charset.forName("UTF-8").newDecoder
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    //use GZIP input stream to unzip the .gz so we can use fromInputStream to convert to String with mkString
    val warcContent: String =
      scala.io.Source.fromInputStream(
        new GZIPInputStream(s3WarcObject.getObjectContent())
      ).mkString
    //release the resources
    s3WarcObject.close()

    warcContent
  }

  /**
   * Converts the warc filename path to a wet filename path
   * @param filename = warc filename path excluding root object
   * @return = String of wet file name path
   */
  def getWetFilename(filename: String):String = {
    filename.replace("/warc/", "/wet/").replace("warc.gz", "warc.wet.gz")
  }


}
