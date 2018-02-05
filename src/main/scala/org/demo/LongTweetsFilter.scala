package org.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object LongTweetsFilter {
  def filterLongTweets(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.select("tweet").filter(_.length > 144)
  }
}
