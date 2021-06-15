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

import java.util.zip.GZIPInputStream


object Runner {
  final val RUN_ON_EMR = false;

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

    val commonCrawlDF = spark.read
      .option("inferSchema", "true")
      .option("header", true)
      .load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    //s3a://maria-batch-1005/athena/

    //Amazon S3 Setup
    var s3Client: AmazonS3 = null;
    if(!RUN_ON_EMR){
      val s3Creds = new BasicAWSCredentials(keys.AccessKey, keys.SecretKey)
      s3Client =
        AmazonS3ClientBuilder
          .standard()
          .withCredentials(new AWSStaticCredentialsProvider(s3Creds))
          .withRegion("us-east-1")
          .build()
    }
    val s3OutputBucket = "s3a://franciscotest/"
    val crawl = "CC-MAIN-2021-21"
    val subset = "warc"

    //"crawl-data/CC-MAIN-2018-09/segments/1518891812584.40/warc/CC-MAIN-20180219111908-20180219131908-00636.warc.gz"
    val jobDomainList = List[String]("indeed.com")
    val filteredCommonCrawlDF = commonCrawlDF
      .filter( $"crawl" === crawl)
      .filter($"fetch_status" === 200)
      .filter($"subset" === subset)
      //.filter($"content_languages".like("%eng%"))
      .filter($"url_host_registered_domain".isin(jobDomainList:_*))
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
      .limit(100)


    val warcFilesInfo =
      filteredCommonCrawlDF
        .select("warc_filename", "warc_record_offset", "warc_record_length")
        .map(row=> (row.getString(0), row.getInt(1), row.getInt(2)))
        .take(10)
        .toList

    warcFilesInfo.foreach(
      warcFile => {
        val warcContent: String = getContentFromS3Object(warcFile._1, warcFile._2, warcFile._3, s3Client)
        println(" NEWWWWW  WARC CONTENT")
        println(warcContent)

        /**
        var wetExtracts =
          spark.read.option("lineSep", "Content-Length: ").textFile(crawlFilepath)
            .filter(
              lower($"value").contains("job") ||
              lower($"value").contains("career") ||
              lower($"value").contains("programmer") ||
              lower($"value").contains("engineer") ||
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
              lower($"value").contains("analyst")
            )
        wetExtracts.take(10).foreach(println)
         **/
      }
    )

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
    def getContentFromS3Object(filename: String, fileOffset: Int, fileLength: Int, s3Client : AmazonS3): String = {
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



    spark.close()
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
