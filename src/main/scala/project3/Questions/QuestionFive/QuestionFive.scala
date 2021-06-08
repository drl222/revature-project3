package project3.Questions.QuestionFive

import org.apache.spark.sql.functions.{col, lower, trunc}
import org.apache.spark.sql.{DataFrame, SparkSession}
import project3.daos.warcToDS.warcToDS

object QuestionFive {
  //What percent of tech job posters post no more than three job ads a month?
  def questionFive (spark : SparkSession, commonCrawlDF : DataFrame) : Unit = {

    //Make a different object that allows me to get a warc.gz, the one below I got through Athena query.
    //s3a://commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084891316.80/warc/CC-MAIN-20180122113633-20180122133633-00510.warc.gz

    import spark.implicits._
    //val testWarc = "s3a://commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084891316.80/warc/CC-MAIN-20180122113633-20180122133633-00510.warc.gz"
    val testWarc = "s3a://commoncrawl/crawl-data/CC-MAIN-2018-05/segments/1516084888077.41/warc/CC-MAIN-20180119164824-20180119184824-00514.warc.gz"
    //

    val crawl = "CC-MAIN-2018-05"
    val indeedJobWarc = commonCrawlDF
      //trunc(col("fetch_time"),"Month").as("Month_Trunc")
      //.select("url", "warc_filename")
      .select($"url",$"warc_filename", $"fetch_time") //only does the first row :( trunc(col("fetch_time"),"Month").as("Month_Trunc"))
      .filter($"crawl" === crawl)
      .filter($"subset" === "warc")
      .filter($"url_host_tld" === "com")
      .filter($"url_host_registered_domain" === "indeed.com")
      .filter($"url".contains("indeed.com/cmp/"))
      .filter(lower($"url_path").contains("jobs"))
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
        lower($"url_path").contains("analyst"))
      .limit(100)

    //indeedJobWarc.show(100, false)

    //test sirius-computer-solutions
    // crawl-data/CC-MAIN-2018-05/segments/1516084888077.41/warc/CC-MAIN-20180119164824-20180119184824-00514.warc.gz
    //Test warc file into a CSV
    warcToDS(spark, testWarc)
      .filter($"value".contains("indeed.com/cmp/"))
      .write.csv("hdfs://localhost:9000/user/arielrubio/commoncrawl/warc6.csv")

  }
}
