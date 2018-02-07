package org.demo

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

class LongTweetsFilterSpec extends FunSuite with BeforeAndAfterAll with SparkTemplate with EmbeddedCassandra {
  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  val connector = CassandraConnector(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    connector.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
      session.execute("CREATE TABLE test.long_tweets(timestamp timestamp, username text, tweet text, PRIMARY KEY(timestamp, username));")
    }
  }

  test("Should filter tweets longer than 144 chars") {
    // SparkSession is the entry point for reading data, similar to the old SQLContext.read.
    implicit val spark: SparkSession = sparkSession

    // define a schema for the data in the tweets CSV
    val tweetsSchema = StructType(Array(StructField("timestamp", LongType, true), StructField("username", StringType, true), StructField("tweet", StringType, true)))

    // read the csv
    val lines = sparkSession.read.option("header", "false").schema(tweetsSchema).csv("./src/test/resources/tweets.csv")

    val longTweets = LongTweetsFilter.filterLongTweets(lines)

    longTweets.write.cassandraFormat("long_tweets", "test").save()

    val cassandraLongTweets = spark.read.cassandraFormat("long_tweets", "test").load()

    cassandraLongTweets.collect().foreach(tweet => assert(tweet.length > 144))
  }
}
