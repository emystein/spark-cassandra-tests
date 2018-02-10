package org.demo

import org.apache.spark.sql.{Dataset, SparkSession}

object LongTweetsFilter {
  def filterLongTweets(df: Dataset[Tweet])(implicit spark: SparkSession): Dataset[Tweet] = {
    df.filter(_.text.length > 144)
  }
}
