package project3.Questions

import org.apache.spark.sql.{DataFrame, SparkSession}

package object QuestionFour {
  def questionFour(spark : SparkSession, commonCrawlDF : DataFrame) : Unit = {
    //write an athena query and receive the csv as output

    //load the output csv into a DF

    //use spark.sql to filter the DF

    //.show() our output

  }
}
