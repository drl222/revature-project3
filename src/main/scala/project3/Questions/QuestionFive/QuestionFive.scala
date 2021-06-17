package project3.Questions.QuestionFive

import org.apache.spark.sql.functions.{col, date_format, round}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


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

    val jobSpike = jobCountDF
     jobSpike.createOrReplaceTempView("jobcount")
    spark.sql(" create or replace TEMPORARY view test1 as SELECT sum(url_path_count) as jobSum, month_time FROM jobcount GROUP BY month_time ORDER BY month_time")
    spark.sql("create or replace TEMPORARY view avg as SELECT round(avg(jobSum),2) as jobAvG, case when month_time between '2020-01-01 00:00:00' and '2020-03-01 00:00:00' then 'Q1' when month_time between '2020-04-01 00:00:00' and '2020-06-01 00:00:00' then 'Q2' when month_time between '2020-07-01 00:00:00' and '2020-09-01 00:00:00' then 'Q3'" +
      " when month_time between '2020-10-01 00:00:00' and '2020-12-01 00:00:00' then 'Q4' END AS Quarters FROM test1 group by Quarters order by Quarters")

      spark.sql("create or replace TEMPORARY view sum as SELECT sum(url_path_count) as jobSum, case  when month_time = '2020-03-01 00:00:00' then 'Q1' when month_time = '2020-06-01 00:00:00' then 'Q2' when month_time = '2020-09-01 00:00:00' then 'Q3'" +
      " when month_time = '2020-12-01 00:00:00' then 'Q4'" +
      " end as Quarters FROM jobcount  GROUP BY Quarters having Quarters is not null ORDER BY Quarters")


    val s = spark.sql("select round( s.jobSum/ a.jobAvG * 100 - 100, 2) as jobPostingRate, s.Quarters from sum s join avg a on (s.Quarters = a.Quarters) order by s.Quarters").show()
   // dd.coalesce(1).write.format("csv").option("header", true).mode(SaveMode.Overwrite).save("hdfs://localhost:9000/user/project2/output3.csv")



   // spark.sql("select avg(jobSum) as Q1AVG, Q1 as Quarters from jobpost where month_time = '2020-01-01 00:00:00' or month_time = '2020-02-01 00:00:00' or month_time = '2020-03-01 00:00:00' ").show()


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
    val d = spark.sql("select `Three Jobs or Less`, Round(`Greater Than Three Jobs`, 2) as `greater Than three jobs` , Month from fixdata").show()
    //d.coalesce(1).write.format("csv").option("header", true).mode(SaveMode.Overwrite).save("hdfs://localhost:9000/user/project2/output1.csv")
  }

}
