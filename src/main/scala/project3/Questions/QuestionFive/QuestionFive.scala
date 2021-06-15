package project3.Questions.QuestionFive

import org.apache.spark.sql.functions.{col, date_format, round}
import org.apache.spark.sql.{DataFrame, SparkSession}


object QuestionFive {
  //What percent of tech job posters post no more than three job ads a month?
  def questionFive (spark : SparkSession, commonCrawlDF : DataFrame) : Unit = {

    //Create a DF based of the Athena query that gives the url_host_name, url_path_count, and timestamp (divided by months)
    val jobCountDF = {
      spark.read
        .option("inferSchema", "true")
        .option("header", true)
        .format("csv")
        .load("src/main/scala/project3/Questions/QuestionFive/JobCountPerMonth_2020.csv")
    }
    //git jobCountDF.show(1000, false)

    jobCountDF.createOrReplaceTempView("vJobCountView")

    val jobCount = spark.sql(
      """
        |SELECT
        |count(case WHEN url_path_count <= 3 THEN 1 else null end) as threeOrLess,
        |count(case WHEN url_path_count > 3 THEN 1 else null end) as greaterThan3,
        |month_time
        |FROM vJobCountView
        |GROUP BY month_time
        |ORDER BY month_time
        |""".stripMargin
    )

    //Format the time to MMMM yyyy
    //Turn the columns threeOrLess and greaterThan3 into percentages
    val jobCountv2 = jobCount
      .withColumn("a", col("threeOrLess") /(col("threeOrLess") + col("greaterThan3")))
      .withColumn("b", col("greaterThan3") /(col("threeOrLess") + col("greaterThan3")))
    val jobCountv3 = jobCountv2.select(
      round(col("a"), 2).as("Three Jobs or Less"),
      round(col("b"), 2).as("Greater Than Three Jobs"),
      date_format(col("month_time"), "MMMM yyyy").as("Month")
    )
    jobCountv3.createOrReplaceTempView("jobs")

    spark.sql("create or replace TEMPORARY  view fixdata as select `Three Jobs or Less` *100 as `three jobs or less`, `Greater Than Three Jobs` *100 as `Greater Than Three Jobs`, Month from jobs")
    spark.sql("select `Three Jobs or Less`, Round(`Greater Than Three Jobs`, 2), Month from fixdata").show

  }

}
