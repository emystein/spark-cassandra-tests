# Testing Spark and Cassandra integration in Scala
In this article I will show you how to run a **Apache Spark** job integrated with an **Apache Cassandra** database. I will also show how to test the Spark and Cassandra integration in a local test environment that provides embedded Spark and Cassandra instances.

The code in this article is written in **Scala**, but since Spark provides bindings for Java, Python for other languages, the examples presented here can be implemented in such languages as well.

## Prerequisites
If you want to run the code in this article you will need **Scala** and **SBT** installed on your system. For details you can read [here](https://www.scala-lang.org/documentation/getting-started-sbt-track/getting-started-with-scala-and-sbt-on-the-command-line.html).

## Introduction to Cassandra
Apache Cassandra is a distributed, scalable, highly-available, fault-tolerant NoSQL database recommended for storing massive amounts of data. Currently it serves many well established internet-scale services.

It is worth to note that Cassandra does not provide many features found in relational databases, like referential integrity or join queries. On the other hand, Cassandra supports storing high volumes of data, geographically distributed and with fault tolerance and relative low maintenance cost, all features that are difficult to achieve using SQL databases at the scales Cassandra can work on.

In Cassandra, tables live in a keyspace, similar to a SQL schema. Cassandra also provides a query language called CQL, which resembles SQL but just in the simplest data access cases like selecting data from a single table, or insert and update mutations.

The NoSQL nature of Cassandra makes it difficult to resolve some queries, depending on how the data is modeled. The way you model the data in Cassandra influences the queries you can run on that data.  

For example, contrary to a SQL database, running a grouping query on Cassandra is not possible and other tools needs to be used for the task, like Spark, to pre-process the data and store it in query-specific Cassandra tables. So a common use case is to have denormalized data duplicated in many different Cassandra tables, every table modeled to resolve a specific query.

## Introduction to Spark
Apache Spark is a framework for modeling and running distributed computations.

Spark introduces the concept of `RDD` (Resilient Distributed Dataset), an immutable fault-tolerant, distributed collection of objects that can be operated on in parallel by Spark high-order functions like map, filter, etc.

In other words, with Spark you compose functions like you would do with regular Scala or Java 8 lambdas but the functions operates on RDDs, and all this happen in a distributed computation deployment. 

An RDD can contain any type of object and is created by loading an external dataset such as text files, database tables or even consuming message queues.

Since version 1.6.0 of Spark, two classes were added similar to RDD, the `DataFrame` and `Dataset`, which allows to model data organized in columns, like database table or CSV files. The main difference between DataFrame and Dataset is that the Dataset is a parameterized type `Dataset<T>` while DataFrame works on generic Row objects. Moreover, in Spark 2.0 DataFrame was redefined as just an alias of `Dataset<Row>` in the Java and Scala APIs. 

# Processing Cassandra data with Spark
The Spark job presented in this article will read a CSV file containing tweets and will store in a Cassandra table the tweets that are longer than 144 characters.

The input `tweets.csv` contains three columns: Tweet timestamp, Username and Tweet text.

Sample snippet:

```
1,emenendez,This is my first tweet!
2,emenendez,Apache Spark is a fast and general-purpose cluster computing system.
3,emenendez,Go Spurs go!!!
```

The pseudo-code of the job is:

```
Read tweets.csv into a DataFrame representing the CSV rows

Select the DataFrame rows which the "text" column has a length longer than 144 characters

Persist the selected rows into Cassandra
```

## Cassandra tables
The keyspace creation:

`
CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
`

I let the details about the keyspace creation for another article.

The **long_tweets** table creation:

`
CREATE TABLE test.long_tweets(timestamp timestamp, username text, text text, PRIMARY KEY(timestamp, username));
`

# Integrating Spark and Cassandra

## Embedded Spark and Cassandra
In order to be able to use embedded Spark and Cassandra, the dependencies needed are the artifacts `com.datastax.spark:spark-cassandra-connector-embedded` and `org.apache.cassandra:cassandra-all`

This project is built using **SBT**. Below is the dependency declaration of the build descriptor build.sbt.

In my experiments I realized that the embedded Cassandra was not able to run if two conflicting SLF4J binding were found in the dependencies of the project, they were not added as direct dependencies but as transitive ones. So I excluded one of them, I chose the log4j-over-slf4j dependency for exclusion and after doing that I got a running embedded Cassandra like I wanted.

```
libraryDependencies ++= Seq(
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test",
  "com.datastax.spark" %% "spark-cassandra-connector-unshaded" % sparkCassandraConnectorVersion,
  "com.datastax.spark" %% "spark-cassandra-connector-embedded" % sparkCassandraConnectorVersion % "test",
  "org.apache.cassandra" % "cassandra-all" % cassandraVersion % "test"
).map(_.exclude("org.slf4j", "log4j-over-slf4j")) // Excluded to allow Cassandra to run embedded
```

## Scala test

We will try the integration between Spark and Cassandra with a Scala test. The declaration of the test class includes the code that runs the embedded Spark and Cassandra:

```
class LongTweetsFilterSpec extends FunSuite with BeforeAndAfterAll with SparkTemplate with EmbeddedCassandra {
```

Extending `FunSuite` declares this class as a test class, `SparkTemplate` runs a Spark context during the tests, and `EmbeddedCassandra` runs (as you may imagine) an embedded Cassandra instance during the life of the tests. 

## Spark components
`SparkSession` is the entry point for reading data. SparkSession is responsible to load the **tweets.csv** into a `Dataset` for later consumption by the `LongTweetsFilter` which is the object that implements the logic of filtering tweets longer than 144.

```
// this allows encoders for types other than basic, like Tweet
import spark.implicits._

val lines = sparkSession.read.option("header", "false").schema(tweetsSchema).csv("./src/test/resources/tweets.csv").as[Tweet]
```

We create a case class to be used as objects part of Datasets:

```
case class Tweet(timestamp: Long, username: String, text: String);
```

The static declaration of the CSV schema which allows to access the CSV data as a DataFrame/Dataset: 

```
val tweetsSchema = StructType(Array(StructField("timestamp", LongType, true), StructField("username", StringType, true), StructField("tweet", StringType, true)))
```

allows to use the DataFrame for selecting data by column name:

```
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LongTweetsFilter {
  def filterLongTweets(df: Dataset[Tweet])(implicit spark: SparkSession): Dataset[Tweet] = {
    df.filter(_.text.length > 144)
  }
}
```

## Cassandra connector

Adding the following imports to the test class

```
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
```

allows to interact Spark RDDs, DataFrames or Datasets with Cassandra.

In order to persist the filtered tweets into Cassandra, first we create the Cassandra keyspace and table before running any test:

```
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
      session.execute("CREATE TABLE test.long_tweets(timestamp timestamp, username text, text text, PRIMARY KEY(timestamp, username));")
    }
  }
```

Then for filtering the long tweets we process the DataFrame with LongTweetsFilter:

```
    // this allows encoders for types other than basic, like Tweet
    import spark.implicits._

    val lines = sparkSession.read.option("header", "false").schema(tweetsSchema).csv("./src/test/resources/tweets.csv").as[Tweet]

    val longTweets = LongTweetsFilter.filterLongTweets(lines)
```

And finally, after filtering the long tweets with LongTweetsFilter they can be persisted in the Cassandra table created for storing them:

```
    longTweets.write.cassandraFormat("long_tweets", "test").save()

    // assert long tweets were finally persisted into the cassandra table
    val cassandraLongTweets = spark.read.cassandraFormat("long_tweets", "test").load()
    cassandraLongTweets.collect().foreach(tweet => assert(tweet.length > 144))
```

## Running the tests from the terminal

`sbt clean test`

and you should get at the end something like this:

```
[info] LongTweetsFilterSpec:
[info] - Should filter tweets longer than 144 chars
[info] ScalaTest
[info] Run completed in 14 seconds, 8 milliseconds.
[info] Total number of tests run: 1
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[info] Passed: Total 1, Failed 0, Errors 0, Passed 1
[success] Total time: 23 s, completed Feb 7, 2018 9:52:44 AM
```

# Complete project
Complete code project can be found at Github: https://github.com/emystein/spark-cassandra-tests

# Further reading
[A Tale of Three Apache Spark APIs: RDDs, DataFrames, and Datasets](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)

[Spark SQL and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes)

[Datastax Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector)

