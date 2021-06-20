package project3.Questions.QuestionOne

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Question1 {
  def getEntryLevelResults(dataDF: Dataset[Row], spark: SparkSession) = {
    val jobsDF = dataDF
      .select("Experience", "Entry Level")
      .filter(col("Entry Level") === true)
      .groupBy("Entry Level", "Experience")
      .count()
      .cache()
    val entryNoExp = jobsDF
      .filter(col("Entry Level") === true && col("Experience") === "0")
      .count
      .toDouble
    val entryExpRequired = jobsDF
      .filter(col("Entry Level") === true && col("Experience") =!= "0")
      .count
      .toDouble
    val percentExpRequired = spark.sparkContext.parallelize(Seq(entryExpRequired / (entryExpRequired + entryNoExp)))

    jobsDF.unpersist()
    (jobsDF, percentExpRequired)
  }

  def getEntryLevelResults(s3Link: String, spark: SparkSession): (DataFrame, RDD[Double]) = {
    getEntryLevelResults(spark.read.load(s3Link), spark)
  }

  def showResults(dataDF: Dataset[Row], spark: SparkSession) = {
    val (jobsDF, percentExpRequired) = getEntryLevelResults(dataDF, spark)
    val percentage = percentExpRequired.collect()(0)
    jobsDF.show
    println(s"Entry Level Experience Required Percentage: $percentage")
  }

  def showResults(s3Link: String, spark: SparkSession) = {
    val (jobsDF, percentExpRequired) = getEntryLevelResults(s3Link, spark)
    val percentage = percentExpRequired.collect()(0)
    jobsDF.show
    println(s"Entry Level Experience Required Percentage: $percentage")
  }

  def storeResultsToS3(s3Link: String, jobsDF: Dataset[Row], entryExpReqPercentage: RDD[Double]): Unit = {
    jobsDF.coalesce(1).write.format("csv").option("delimiter", ",").save(s3Link + "/Q1/Jobs")
    entryExpReqPercentage.saveAsTextFile(s3Link + "/Q1/Percentage")
  }
}
