package org.demo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LongTweetsFilter {
  def filterLongTweets(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.where(length(df("tweet")) > 144)
  }
}
