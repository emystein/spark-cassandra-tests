Integrating Spark and Cassandra
===============================
In this article I will show you how to run a Apache Spark job integrated with a Cassadnra database. I will also show how to test the Spark and Cassandra integration in a local test environment that provides embedded Spark and Cassandra instances.

The code in this article is written in Scala, but since Spark provides bindings for Java, Python for other languages, the examples presented here can be implemented in such languages as well.


Introduction to Cassandra
-------------------------
Apache Cassandra is a distributed, scalable, highly-available, fault-tolerant NoSQL database recommended for storing massive amounts of data. Currently it serves many well established internet-scale services.

It is worth to note that Cassandra does not provide many features found in relational databases, like referential integrity or join queries. On the other hand, Cassandra supports storing high volumes of data, geographically distributed and with fault tolerance and relative low maintenance cost, all features that are difficult to achieve using SQL databases at the scales Cassandra can work on.

In Cassandra, tables live in a keyspace, similar to a SQL schema. Cassandra also provides a query language called CQL, which resembles SQL but just in the simplest data access cases like selecting data from a single table, or insert and update mutations.

The NoSQL nature of Cassandra makes it difficult to resolve some queries, depending on how the data is modeled. The way you model the data in Cassandra influences the queries you can run on that data.  

For example, contrary to a SQL database, running a grouping query on Cassandra is not possible and other tools needs to be used for the task, like Spark, to pre-process the data and store it in query-specific Cassandra tables. So a common use case is to have denormalized data duplicated in many different Cassandra tables, every table modeled to resolve a specific query.


Introduction to Spark
---------------------
Apache Spark is a framework for modeling and running distributed computations.

Spark introduces the concept of RDD (Resilient Distributed Dataset), an immutable fault-tolerant, distributed collection of objects that can be operated on in parallel by Spark high-order functions like map, filter, etc.

In other words, with Spark you compose functions like you would do with regular Scala or Java lambdas but the functions operates on RDDs, and all this happen in a distributed computation deployment. 

An RDD can contain any type of object and is created by loading an external dataset such as text files, database tables or even consuming message queues.

Since version 1.6.0 of Spark, two classes were added similar to RDD, the Dataset and DataFrame, which allows to model data organized in columns, like database table or CSV files. The main difference between DataFrame and Dataset is that the Dataset is a parameterized type Dataset<T> while DataFrame works on generic Row objects. Moreover, in Spark 2.0 DataFrame was redefined as just an alias of Dataset in the Java and Scala APIs. 


Processing Cassandra data with Spark
====================================
The job presented in this article will read a CSV file containing tweets and will store in a Cassandra table the tweets that are longer than 144 characters.

The input tweets.csv contains three columns: Tweet timestamp, Username and Tweet text.

Sample snippet:

```
1,emenendez,This is my first tweet!
2,emenendez,Apache Spark is a fast and general-purpose cluster computing system.
3,emenendez,Go Spurs go!!!
```

Cassandra tables
----------------
The keyspace creation:

`
CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
`

I let the details about the keyspace creation for another article.

The long_tweets table creation:

`
CREATE TABLE long_tweets(timestamp timestamp, username text, tweet text, PRIMARY KEY(timestamp, username));
`

Running Spark in Scala tests
============================
The full code of the test which shows the integration between Spark and Cassandra, filtering tweets and sending the longer than 144 to Cassandra for persistence:

`
TODO: ADD FULL CODE OF TEST
`

Embedded Spark and Cassandra
----------------------------
For being able to use embedded Spark and Cassandra, the dependency artifacts com.datastax.spark:spark-cassandra-connector-embedded and org.apache.cassandra:cassandra-all

The declaration of the test class is powerful: extending FunSuite declares this class as a test class, SparkTemplate runs a Spark context during the tests, and EmbeddedCassandra runs (as you may imagine) an embedded Cassandra instance during the life of the tests. 

Spark components
----------------
SparkSession is the entry point for reading data. In this article, SparkSession is responsible to load the tweets.csv into a DataFrame for later consumption by the LongTweetsFilter which is the object that implements the logic of filtering tweets longer than 144.

The static declaration of the CSV schema which allows to access the CSV data as a DataFrame: 

`
val tweetsSchema = StructType(Array(StructField("timestamp", LongType, true), StructField("username", StringType, true), StructField("tweet", StringType, true)))
`

allows to use the DataFrame for selecting data by column name:

`
object LongTweetsFilter {
  def filterLongTweets(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.select("tweet").filter(_.length > 144)
  }
}
`


SBT
---
The project for this article is built using SBT. You can find the full build descriptor in the linked Github project but I want to add quick note about the build and the embedded Cassandra:

In my experiments I realized that the embedded Cassandra was not able to run if two conflicting SLF4J binding were found in the dependencies of the project, they were not added as direct dependencies but as transitive ones. So I excluded one of them, I chose the log4j-over-slf4j dependency for exclusion and after doing that I got a running embedded Cassandra like I wanted.

`
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
`

Further reading
===============

