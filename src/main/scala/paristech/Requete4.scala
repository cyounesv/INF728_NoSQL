package paristech
import org.apache.spark
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.functions.udf
import com.datastax.spark.connector._

import org.apache.spark.sql.types.IntegerType
import paristech.EventMentionETL.spark

object Requete4 extends App  {

  Logger.getLogger("akka").setLevel(Level.WARN)

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
    "spark.master" -> "local[*]",
    "spark.cassandra.connection.connections_per_executor_max" -> "2"))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("TP Spark : Trainer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.setCassandraConf("Test", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1"))

  def toMonthYear(date: String, size: Int) = {
    //date.toString().take(size).toInt
    date.take(size)
  }
  val date = udf(toMonthYear _)

  def orderCountries(country1: String, country2: String):Array[String]= {
    var country3 = country1
    var country4 = country2
    if(country2<country1){
      country3 = country2
      country4 = country1
    }
    Array(country3, country4)
  }

  val orderCountriesCol = udf(orderCountries _)

  val getOutputReq2Map = spark.sqlContext.read.format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "requete2mapping", "keyspace" -> "nosql", "cluster" -> "test"))
    .load

  val countriesRelations = getOutputReq2Map.filter(col("actor1countrycode")=!= "" && col("actor2countrycode")=!= "")
    //.filter(col("actor2countrycode").isNotNull )
    .withColumn("countries", orderCountriesCol(col("actor1countrycode"), col("actor2countrycode")))
    .select(col("countries")(0).as("pays1"), col("countries")(1).as("pays2"), col("day"), col("count"), col("sumtone"))
   // .select("day", "count", "sumtone")
    .withColumn("monthyear", date(col("day"), lit(6)))
    .withColumn("year", date(col("day"), lit(4)))
    //.select("year", "monthyear","day", "count","sumtone", "pays1", "pays2")
    .groupBy("pays1", "year", "monthyear", "day", "pays2").agg(mean("sumtone"), sum("count"))
      .withColumnRenamed("avg(sumtone)", "averagetone").withColumnRenamed("sum(count)", "numberofarticles")


  countriesRelations.show(10, false)

  countriesRelations.write
    .cassandraFormat("req41", "nosql", "test")
    .mode(SaveMode.Append)
    .save()

  val countriesRelationsReverses = countriesRelations.withColumn("pays3", col("pays2"))
    .withColumn("pays4", col("pays1"))
    .drop("pays1", "pays2")
    .withColumnRenamed("pays3", "pays1")
    .withColumnRenamed("pays4", "pays2")

/*val countriesAllRelations = countriesRelations.union(countriesRelationsReverses)
  countriesRelationsReverses.show(10, false)
  countriesAllRelations.show(10, false)
*/
  countriesRelationsReverses.write
    .cassandraFormat("req41", "nosql", "test")
    .mode(SaveMode.Append)
    .save()


}