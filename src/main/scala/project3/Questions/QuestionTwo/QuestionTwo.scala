package project3.Questions.QuestionTwo

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark._

import java.io.PrintWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.Map
import scala.util.matching.Regex
import java.io.File
import java.io.PrintWriter
import java.time.LocalDateTime
import org.apache.spark.sql.functions.{col, lower, trunc}
import org.apache.spark.sql.functions.col

object QuestionTwo {
  def questionTwo(spark: SparkSession, Q2DF: DataFrame): Unit = {




    import spark.implicits._
     val q = Q2DF.select(col("Company"),col("Jobtitle")).filter(lower(col("Jobtitle")).contains("programmer") ||
      lower(col("Jobtitle")).contains("engineer") ||
      lower(col("Jobtitle")).contains("software") ||
      lower(col("Jobtitle")).contains("computer") ||
      lower(col("Jobtitle")).contains("developer") ||
      lower(col("Jobtitle")).contains("java") ||
      lower(col("Jobtitle")).contains("information technology") ||
      lower(col("Jobtitle")).contains("it specialist") ||
      lower(col("Jobtitle")).contains("tech support") ||
      lower(col("Jobtitle")).contains("technical support") ||
      lower(col("Jobtitle")).contains("network") ||
      lower(col("Jobtitle")).contains("technician") ||
      lower(col("Jobtitle")).contains("analyst"))

    q.createOrReplaceTempView("job")

    val d = spark.sql("select Company, count(Company) as count from job group by Company order by count DESC limit 3")
    d.coalesce(1).write.format("csv").option("header", true).mode(SaveMode.Overwrite).save("hdfs://localhost:9000/user/project2/output9.csv")


  }

}
