package project3


import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{GetObjectRequest, S3ObjectInputStream}

import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CodingErrorAction
import keys.keys
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{lower, when}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.util.zip.GZIPInputStream


object Runner {
  final val RUN_ON_EMR = false

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
      .builder()
      //.master("local[*]")
      .appName("francisco extract")
      //.config("spark.hadoop.fs.s3a.access.key", keys.AccessKey)
      //.config("spark.hadoop.fs.s3a.secret.key", keys.SecretKey)
      //.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      //.config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
      //.config("spark.hadoop.fs.s3a.fast.upload","true")
      //.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    /**

    val commonCrawlDF = spark.read
      .option("inferSchema", "true")
      .option("header", true)
      .load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    val s3OutputBucket = "s3a://franciscotest/"
    val crawl = "CC-MAIN-2021-21"
    val subset = "warc"

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
    /**
    val  customSchema = StructType(
      Seq(
        StructField("warc_filename", StringType, true),
        StructField("warc_record_offset", IntegerType, true),
        StructField("warc_record_length", IntegerType, true),
        StructField("timestamp", TimestampType, true),
        StructField("url", StringType, true)
      )
    ) **/
    //s3a://maria-batch-1005/athena/small-testing/indeed_sample.txt
    //s3a://maria-batch-1005/large_test
    // s3a://maria-batch-1005/test_2015_2021
    /**
    val filteredCommonCrawlDF = {
      spark.read.format("parquet")
        .schema(customSchema)
        .load("s3a://maria-batch-1005/large_test")

    } **/


    val filteredCommonCrawlDF = spark.read.format("parquet").option("inferschema","true").load("s3a://maria-batch-1005/test_2015_2021")

    //filteredCommonCrawlDF.show(false)


    val warcFilesInfo =
      filteredCommonCrawlDF
        .select("warc_filename", "warc_record_offset", "warc_record_length")

   // warcFilesInfo.show()


    //(?s) in regex allows the regex to include newlines in the . operator e.g:(.*? will include newlines now)
    //<.*? =\"date\" .*? >(.*>)<.*?>
    val company_re = "(?s)<.*?=\"[cC]ompany(?:[nN]ame)?\".*?>(.*?)</.*?>".r
    val companyInner_re = "(?s)(?:<.*>)\\s*(.*)".r
    val jobtitle_re = "(?s)<.*?=\"[jJ]ob[tT]itle\">(.*?)</.*?>".r
    val jobtitleInner_re = "(?s)(?:<.* title=\"(.*?)\".*>)".r
    val location_re = "(?s)<.*?=\"location\">(.*?)</.*?>".r
    val description_re = "(?s)(?:<.*?=\"job_summary\" class=\"summary\">(.*?)</.*?>)|(?:<div id=\"jobDescriptionText\" class=\"jobsearch-jobDescriptionText\">(.*?)</div>)".r
    val time_re = "(?s)(?:<span class=\"date\">(.*?)</span>)|(?:<div class=\"jobsearch-JobMetadataFooter\">\\s*<div>(.*?)</div>)|(?:<span class=\"old-date\">(.*?)</span>)".r
    val experience_re = "(?s)([0-9]+)\\+? [Yy]ears? ?.*? [Ee]xperience".r
    val entryLevel_re = "(?s)[eE]ntry".r

    val dataRetrievalDF = warcFilesInfo.map(
      warcRow => {
        val warcContent: String = S3Serializable.getContentFromS3Object(warcRow.getString(0), warcRow.getInt(1), warcRow.getInt(2))
        var company = ""
        var jobTitle = ""
        var description = ""
        var location = ""
        var time = ""
        var experience = ""
        var entryLevel = false

        company_re.findFirstMatchIn(warcContent) match {
          case Some(value) => {
            company = value.group(1)
            companyInner_re.findFirstMatchIn(company) match{
              case Some(extract) => company = extract.group(1)
              case None => //do nothing, retain previous value
            }
          }
          case None => {
            company = "N/A"
          }
        }

        jobtitle_re.findFirstMatchIn(warcContent) match {
          case Some(value) => {
            jobTitle = value.group(1)
            jobtitleInner_re.findFirstMatchIn(jobTitle) match{
              case Some(extract) => jobTitle = extract.group(1)
              case None => //do nothing, retain previous value
            }
          }
          case None => {
            jobTitle = "N/A"
          }
        }

        description_re.findFirstMatchIn(warcContent) match {
          case Some(value) => {
            description = if (value.group(1) == null) {value.group(2)} else {value.group(1)}
          }
          case None => {
            description = "N/A"
          }
        }

        experience_re.findFirstMatchIn(if(description != "N/A")description else warcContent) match {
          case Some(value) =>{
            experience = value.group(1)
          }
          case None => {
            experience = "N/A"
          }
        }

        //checking for Entry Level keywords in Job Title and page
        //) jobTitle else warcContent
        entryLevel_re.findFirstMatchIn(jobTitle) match {
          case Some(value) =>{
            //if(value.group(1) != null)
              entryLevel = true
          }
          case None => //entry level is already false
        }

        location_re.findFirstMatchIn(warcContent) match {
          case Some(value) => location = value.group(1)
          case None => {
            location = "N/A"
          }
        }

        time_re.findFirstMatchIn(warcContent) match {
          case Some(value) => {
            time = value.group(1)
             if (time == null)
               time = value.group(2)
             if(time == null)
               time = value.group(3)
          }
          case None => {
            time = "N/A"
          }
        }


        (company.trim,jobTitle.trim, experience.trim, entryLevel, location.trim, time.trim)
        //warcContent

      }
    )
      .withColumnRenamed("_1","Company")
      .withColumnRenamed("_2","Job Title")
      .withColumnRenamed("_3", "exp")
      .withColumnRenamed("_4", "Entry Level")
      .withColumnRenamed("_5","Location")
      .withColumnRenamed("_6","Time")

    val postProcessedDF = dataRetrievalDF
      .withColumn("Experience",
        when($"Entry Level" === true && $"exp" === "N/A","0").otherwise($"exp"))
      .drop("exp")

    /**val largestJobSeekers = postProcessedDF
      .select("Company","Job Title")
      .filter($"Company" =!= "N/A"|| $"Job Title" =!= "N/A")
    **/

    //largestJobSeekers.show(false)
    postProcessedDF.write.mode("overwrite").csv("s3a://maria-batch-1005/Project3Output/DylanOutput")
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


} //end runner



//to make it Serializable for the Worker Nodes when in Cluster Distributive Environment by making it static
//https://www.nicolaferraro.me/2016/02/22/using-non-serializable-objects-in-apache-spark/
object S3Serializable {
  val s3Creds = new BasicAWSCredentials(keys.AccessKey, keys.SecretKey)
  val s3Client =
    AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(s3Creds))
      .withRegion("us-east-1")
      .build()

  /**
   * getContentFromS3Object utilizes the Amazon S3 Java API to specifically traverse the Common Crawl Index by
   * using the withRange function of S3Objects to skip and point to the correct Warc Page content (using file offset and length)
   * within the accumulated file (@param filename) that contains multiple warc pages.
   * @param filename = the filename path excluding the root bucket
   * @param fileOffset = the respective file offset for fileName
   * @param fileLength = the respective file length for fileName
   * @return = a String of the entire content of the fileName
   */
  def getContentFromS3Object(filename: String, fileOffset: Long, fileLength: Long): String = {
    //https://stackoverflow.com/questions/24537884/how-to-get-first-n-bytes-of-file-from-s3-url
    //https://stackoverflow.com/questions/13625024/how-to-read-a-text-file-with-mixed-encodings-in-scala-or-java
    val s3Client = S3Serializable.s3Client
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

}

