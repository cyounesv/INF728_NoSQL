package paristech
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.sql.functions._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode

object GKG_ETL extends App {

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
    "spark.master" -> "local[*]"))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("TP Spark : Trainer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  /**
   * Je suppose le chargement dans le repertoire /tmp
   *
   */
  case class Gkg(
                  GKGRECORDID: String,
                  DATE: BigInt,
                  SourceCollectionIdentifier: Int,
                  SourceCommonName: String,
                  DocumentIdentifier: String,
                  Counts: String,
                  V2Counts: String,
                  Themes: String,
                  V2Themes: String,
                  Locations: String,
                  V2Locations: String,
                  Persons: String,
                  V2Persons: String,
                  Organizations: String,
                  V2Organizations: String,
                  V2Tone: String,
                  Dates: String,
                  GCAM: String,
                  SharingImage: String,
                  RelatedImages: String,
                  SocialImageEmbeds: String,
                  SocialVideoEmbeds: String,
                  Quotations: String,
                  AllNames: String,
                  Amounts: String,
                  TranslationInfo: String,
                  Extras: String)

  val gkgRDD = spark.sparkContext.binaryFiles("/tmp/2019*.gkg.csv.zip", 100).
    flatMap { // decompresser les fichiers
      case (name: String, content: PortableDataStream) =>
        val zis = new ZipInputStream(content.open)
        Stream.continually(zis.getNextEntry).
          takeWhile(_ != null).
          flatMap { _ =>
            val br = new BufferedReader(new InputStreamReader(zis))
            Stream.continually(br.readLine()).takeWhile(_ != null)
          }
    }

  val cachedGkg = gkgRDD.cache // RDD

  def toDouble(s: String): Double = if (s.isEmpty) 0 else s.toDouble

  def toInt(s: String): Int = if (s.isEmpty) 0 else s.toInt

  def toBigInt(s: String): BigInt = if (s.isEmpty) BigInt(0) else BigInt(s)


  val dfGkg = cachedGkg.map(_.split("\t")).filter(_.length == 27).map(
    e => Gkg(
      e(0), toBigInt(e(1)), toInt(e(2)), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15), e(16),
      e(17), e(18), e(19), e(20), e(21), e(22), e(23), e(24), e(25), e(26))).toDF.cache


 /* println("tota")
  dfGkg.show(3)
  val cassandra31 = dfGkg.select("SourceCommonName").distinct();
  cassandra31.show()
  println("tata")*/

 /* val dfGkgExploded1 = dfGkg.select(($"V2Themes").split(","))
  val dfGkgExploded2 = dfGkg.select($"SourceCommonName", $"Date", explode($"V2Persons"), $"V2Tone")
  val dfGkgExploded3 = dfGkg.select($"SourceCommonName", $"Date", explode($"V2Locations"), $"V2Tone")*/

  def getTone(tones: String): Double = {
    tones.split(",")(0).toDouble
  }
val udfTone = udf(getTone _)

val df31 = dfGkg.select("GKGRECORDID","SourceCommonName", "Date", "V2Themes", "V2Tone")
                      //.groupBy("SourceCommonName", "V2Themes","Date", "V2Tone")
                      .withColumn("Type", lit("Themes"))
                      .withColumn("Themes", split($"V2Themes", ";"))
                      .withColumn("Tone", udfTone($"V2Tone"))
                     //.withColumn("Tone", $"Splitted_tones")


 // df31.show(20, false)

 // val test = df31.select("Splitted_tones").map(x => x(0))
  //test.show(10, false)

  val cassandra31 = df31.select($"GKGRECORDID", $"SourceCommonName", $"Date", explode($"Themes"), $"Tone")
  cassandra31.show(30, false)

  /*val cassandra32 = dfGkgExploded2.select("SourceCommonName", "Date", "V2Persons", "V2Tone")
                        .groupBy("SourceCommonName", "V2Persons")
                        .count().withColumn("Type", lit("Persons"))

  val cassandra33 = dfGkgExploded3.select("SourceCommonName", "Date", "V2Locations", "V2Tone")
                          .groupBy("SourceCommonName", "V2Locations")
                          .count().withColumn("Type", lit("Locations"))
   */
  //cassandra31.show()
//cassandra31.printSchema()
 /* cassandra32.show()
  cassandra33.show()
*/
  println("END")

}