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
import org.apache.commons.math3.stat.descriptive.moment.Mean
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

  def toInt(s: String): Int = if (s.isEmpty) 0 else s.toInt

  def toBigInt(s: String): BigInt = if (s.isEmpty) BigInt(0) else BigInt(s)


  val dfGkg = cachedGkg.map(_.split("\t")).filter(_.length == 27).map(
    e => Gkg(
      e(0), toBigInt(e(1)), toInt(e(2)), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15), e(16),
      e(17), e(18), e(19), e(20), e(21), e(22), e(23), e(24), e(25), e(26))).toDF.cache


 /* println("tota")

  println("tata")*/

  /*********** UDF **************/

  def getTone(tones: String): Double = {
    tones.split(",")(0).toDouble
  }

  val udfTone = udf(getTone _)

  /************************/

  def getAverageTone(tones: Double): String = if (tones<0) "Neg" else "Pos"

  val udfAvTone = udf(getAverageTone _)

  /************************/

  def getTheme(theme: String): String = {
    theme.split(",")(0)
  }
  val udfTheme = udf(getTheme _)


 /********* Dataframes to get Themes of articles with Date and Tone **********/
  val df31 = dfGkg.select("GKGRECORDID","SourceCommonName", "Date", "V2Themes", "V2Tone")
                      //.groupBy("SourceCommonName", "V2Themes","Date", "V2Tone")
                      .withColumn("Type", lit("Themes"))
                      .withColumn("Theme_tmp", split($"V2Themes", ";"))
                      .withColumn("Tone", udfTone($"V2Tone"))
                     //.withColumn("Tone", $"Splitted_tones")


 // df31.show(20, false)

 // val test = df31.select("Splitted_tones").map(x => x(0))
  //test.show(10, false)

  val cassandra31 = df31.select($"GKGRECORDID", $"SourceCommonName", $"Date", explode($"Theme_tmp"), $"Tone")



  val cassandra311 = cassandra31.select("GKGRECORDID", "SourceCommonName", "Date", "col", "Tone")
                                .withColumn("Theme", split($"col", ",")(0))
                                .drop("col", "V2Tones", "V2Themes", "Theme_tmp")
                                //.withColumn("Theme", udfTheme($"Theme_tmp2"))

  val cassandra312 = cassandra311.select("GKGRECORDID", "SourceCommonName", "Date", "Tone", "Theme").distinct()

  /*********** Requete 3 for Themes ************/
  val requete31 = cassandra312.select("GKGRECORDID", "SourceCommonName", "Date", "Tone", "Theme")
                              .groupBy("GKGRECORDID", "SourceCommonName", "Date", "Theme").agg(mean("Tone")).withColumn("AverageTone", udfAvTone($"avg(Tone)"))


  requete31.show(30, false)

  /************* Dataframes to get Persons from articles with Date and Tone *************/

  val df32 = dfGkg.select("GKGRECORDID","SourceCommonName", "Date", "V2Persons", "V2Tone")
                    .withColumn("Type", lit("Persons"))
                    .withColumn("Persons_tmp", split($"V2Persons", ";"))
                    .withColumn("Tone", udfTone($"V2Tone"))

  val cassandra32 = df32.select($"GKGRECORDID", $"SourceCommonName", $"Date", explode($"Persons_tmp"), $"Tone")

  val cassandra321 = cassandra32.select("GKGRECORDID", "SourceCommonName", "Date", "col", "Tone")
                    .withColumn("Person", split($"col", ",")(0))
                    .drop("col", "V2Tones", "V2Persons", "Persons_tmp")

  val cassandra322 = cassandra321.select("GKGRECORDID", "SourceCommonName", "Date", "Tone", "Person").distinct()

  /*********** Requete 3 for Themes ************/
  val requete32 = cassandra322.select("GKGRECORDID", "SourceCommonName", "Date", "Tone", "Person")
                              .groupBy("GKGRECORDID", "SourceCommonName", "Date", "Person").agg(mean("Tone")).withColumn("AverageTone", udfAvTone($"avg(Tone)"))

  requete32.show(30, false)

  /************* Dataframes to get Locations from articles with Date and Tone *************/

  val df33 = dfGkg.select("GKGRECORDID","SourceCommonName", "Date", "V2Locations", "V2Tone")
    .withColumn("Type", lit("Locations"))
    .withColumn("Locations_tmp", split($"V2Locations", ";"))
    .withColumn("Tone", udfTone($"V2Tone"))

  val cassandra33 = df33.select($"GKGRECORDID", $"SourceCommonName", $"Date", explode($"Locations_tmp"), $"Tone")

  val cassandra331 = cassandra33.select("GKGRECORDID", "SourceCommonName", "Date", "col", "Tone")
    .withColumn("Location", split($"col", "#")(0))
    .drop("col", "V2Tones", "V2Locations", "Locations_tmp")

  val cassandra332 = cassandra331.select("GKGRECORDID", "SourceCommonName", "Date", "Tone", "Location").distinct()

  /*********** Requete 3 for Themes ************/
  val requete33 = cassandra332.select("GKGRECORDID", "SourceCommonName", "Date", "Tone", "Location")
    .groupBy("GKGRECORDID", "SourceCommonName", "Date", "Location").agg(mean("Tone")).withColumn("AverageTone", udfAvTone($"avg(Tone)"))

  requete33.show(30, false)
  //cassandra31.show()
//cassandra31.printSchema()
 /* cassandra32.show()
  cassandra33.show()
*/
  println("END")

}