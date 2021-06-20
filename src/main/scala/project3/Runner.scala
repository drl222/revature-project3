package project3


import awsS3.S3Serializable
import org.apache.spark.sql.{Row, SparkSession}
import project3.Questions.Question1



object Runner {
  final val RUN_ON_EMR = true

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
      .builder()
      //.master("local[*]")
      .appName("Project 3 Final")
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
     //'ATHENA' QUERY
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

    val  customSchema = StructType(
      Seq(
        StructField("warc_filename", StringType, true),
        StructField("warc_record_offset", IntegerType, true),
        StructField("warc_record_length", IntegerType, true),
        StructField("timestamp", TimestampType, true),
        StructField("url", StringType, true)
      )
    )

    val filteredCommonCrawlDF = {
      spark.read.format("parquet")
        .schema(customSchema)
        .load("s3a://maria-batch-1005/large_test")
    //END OF 'ATHENA' QUERY
    **/

    val smallTest = "s3a://maria-batch-1005/athena/small-testing/indeed_sample.txt"
    val largeTest = "s3a://maria-batch-1005/large_test"
    val final2015To2021 = "s3a://maria-batch-1005/test_2015_2021"
    val s3OutputBucket = "s3a://maria-1005-batch/Project3Output/"

    val filteredCommonCrawlDF = spark.read.format("parquet").option("inferschema","true").load(final2015To2021)

    val warcFilesInfo =
      filteredCommonCrawlDF
        .select("warc_filename", "warc_record_offset", "warc_record_length")

    //(?s) in regex allows the regex to include newlines in the . operator e.g:(.*? will include newlines now)
    val company_re = "(?s)<.*?=\"[cC]ompany(?:[nN]ame)?\".*?>(.*?)</.*?>".r
    val companyInner_re = "(?s)(?:<.*>)\\s*(.*)".r
    val jobtitle_re = "(?s)<.*?=\"[jJ]ob[tT]itle\">(.*?)</.*?>".r
    val jobtitleInner_re = "(?s)(?:<.* title=\"(.*?)\".*>)".r
    val location_re = "(?s)<.*?=\"location\">(.*?)</.*?>".r
    val description_re = "(?s)(?:<.*?=\"job_summary\" class=\"summary\">(.*?)</.*?>)|(?:<div id=\"jobDescriptionText\" class=\"jobsearch-jobDescriptionText\">(.*?)</div>)".r
    val time_re = "(?s)(?:<span class=\"date\">(.*?)</span>)|(?:<div class=\"jobsearch-JobMetadataFooter\">\\s*<div>(.*?)</div>)|(?:<span class=\"old-date\">(.*?)</span>)".r
    val experience_re = "(?s)([0-9]+)\\+? [Yy]ears? ?.*? [Ee]xperience".r
    val entryLevel_re = "(?s)[eE]ntry|[Jj](?s:unio)?r|[Tt]ier ?[1I]".r

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
            "[eE]xperience".r.findFirstMatchIn(if(description != "N/A")description else warcContent) match{
              case Some(extract) => experience = "Generic Experience"
              case None =>  experience = "0"
                /**
                "[Nn]o (?:[pP]rior)? ?[Ee]xperience".r.findFirstMatchIn(if (description != "N/A") description else warcContent) match {
                  case Some(exp) => experience = "0"
                  case None => experience = "N/A"
                }**/
              }
          }
        }

        //checking for Entry Level keywords in Job Title and page
        entryLevel_re.findFirstMatchIn(jobTitle) match {
          case Some(value) =>{
            //if(value.group(1) != null)
              entryLevel = true
          }
          case None =>
            if(description != "N/A")
              entryLevel_re.findFirstMatchIn(description) match {
                case Some(extract) => entryLevel = true
                case None => //retain previous value
              }
             /**
            if(experience != "N/A" && experience != "Generic Experience" && experience.toInt < 3)
              entryLevel = true
            **/

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

      }
    )
      .withColumnRenamed("_1","Company")
      .withColumnRenamed("_2","Job Title")
      .withColumnRenamed("_3", "Experience")
      .withColumnRenamed("_4", "Entry Level")
      .withColumnRenamed("_5","Location")
      .withColumnRenamed("_6","Time")







    if(RUN_ON_EMR) {
      val (jobsDF, entryExpReqPercentage) = Question1.getEntryLevelResults(dataRetrievalDF, spark)
      Question1.storeResultsToS3(s3OutputBucket, jobsDF, entryExpReqPercentage)

    }
    else{
      Question1.showResults(dataRetrievalDF,spark)

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


} //end runner




