package paristech
import java.io.BufferedReader
import java.io.InputStreamReader

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.functions.udf
import com.datastax.spark.connector._



object test extends App {
  val conf = new SparkConf().setAll(Map(
    "spark.scheduler.mode" -> "FIFO",
    "spark.speculation" -> "false",
    "spark.reducer.maxSizeInFlight" -> "48m",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max" -> "1g",
    "spark.shuffle.file.buffer" -> "32k",
    "spark.default.parallelism" -> "12",
    "spark.sql.shuffle.partitions" -> "12",
    "spark.driver.maxResultSize" -> "2g",
    "spark.master" -> "local[*]"))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("TP Spark : Trainer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  println("On demarre le chargement")
  spark.setCassandraConf("Test", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1"))


  val test1 = spark.sparkContext.cassandraTable("nosql", "requete2mapping")
    .select("country")
    .where("eventid= '890199722'")

  println(test1.first().getString("country"))
}