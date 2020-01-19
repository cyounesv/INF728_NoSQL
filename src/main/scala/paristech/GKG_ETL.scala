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
                  Extras: String,
                  Year: Int,
                  Month: Int,
                  Day: Int
  )

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

  def toYear(s : String): Int = if (s.isEmpty) 0 else (s.slice(0, 0 + 4)).toInt
  def toMonth(s : String): Int = if (s.isEmpty) 0 else (s.slice(4, 4 + 2)).toInt
  def toDay(s : String): Int = if (s.isEmpty) 0 else (s.slice(6, 6 + 2)).toInt

  val dfGkg = cachedGkg.map(_.split("\t")).filter(_.length == 27).map(
    e => Gkg(
      e(0), toBigInt(e(1)), toInt(e(2)), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15), e(16),
      e(17), e(18), e(19), e(20), e(21), e(22), e(23), e(24), e(25), e(26), toYear(e(1)), toMonth(e(1)), toDay(e(1)))).toDF.cache
  /*********** UDF **************/

  def getTone(tones: String): Double = {
    tones.split(",")(0).toDouble
  } // keep only average Tone from GKG Tone field

  val udfTone = udf(getTone _)

/**** A revoir en fonction des moyennes obtenues sur plusieurs jours pour definir l'interval de Neutralite****/
  def getAverageTone(tones: Double): String = if (tones<0) "Neg" else "Pos"

  val udfAvTone = udf(getAverageTone _)

  /*def getTheme(theme: String): String = {
    theme.split(",")(0)
  }
  val udfTheme = udf(getTheme _)
*/
 /********* Dataframes to get Themes of articles with Date and Tone **********/

  val df3Gkg = dfGkg.select("DocumentIdentifier", "SourceCommonName",  "Year", "Month", "Day", "V2Themes", "V2Tone")
                      .filter(!($"SourceCommonName" === ""))
                      //.groupBy("SourceCommonName", "V2Themes","Date", "V2Tone")
                      .withColumn("Type", lit("Themes"))
                      .withColumn("Theme_tmp", split($"V2Themes", ";"))
                      .withColumn("Tone", udfTone($"V2Tone")) // average tone of the article

  val df3GkgThemes = df3Gkg.select( $"DocumentIdentifier", $"SourceCommonName",$"Year", $"Month", $"Day", explode($"Theme_tmp"), $"Tone") //creation d'une ligne par theme pr chaque article


  val df3GkgCleanThemes = df3GkgThemes.select( "DocumentIdentifier", "SourceCommonName","Year", "Month", "Day",  "col", "Tone")
                              .withColumn("Theme", split($"col", ",")(0)) // Keep only Theme not it's location in the article
                              .drop("col", "V2Tones", "V2Themes", "Theme_tmp")
  //Suppression des doublons theme dans un meme article
/**** Verifier qu'on a bien un distinct que sur les themes lorsque l'on ajoutera d'autres jours de data ****/

  //val df3GkgSourceDistinctThemes = df3GkgCleanThemes.select( "SourceCommonName",  "Year", "Month", "Day",  "Tone", "Theme").distinct().filter(!($"Theme" === ""))
  val df3GkgSourceDistinctThemes = df3GkgCleanThemes.dropDuplicates("DocumentIdentifier", "Theme").filter(!($"Theme" === ""))

/**** Idem Verifier le group by avec plusieurs jours, et surtout veut-on creer un groupby month and year aussi??****/
val df3GkgSourceDistinctThemesAvTone = df3GkgSourceDistinctThemes.select( "SourceCommonName",  "Year", "Month", "Day", "Tone", "Theme")
                              .groupBy( "SourceCommonName",  "Year", "Month", "Day", "Theme").agg(mean("Tone")).withColumn("AverageTone", udfAvTone($"avg(Tone)"))
                              .sortWithinPartitions("SourceCommonName")

  df3GkgSourceDistinctThemesAvTone.show(200, false)
 // val test = df3GkgSourceDistinctThemesAvTone.filter('SourceCommonName' == "scotcampus.com")
  /************* Dataframes to get Persons from articles with Date and Tone *************/

/*val df32 = dfGkg.select("SourceCommonName", "Year", "Month", "Day", "V2Persons", "V2Tone")
                    .filter(!($"SourceCommonName" === ""))
                    .withColumn("Type", lit("Persons"))
                    .withColumn("Persons_tmp", split($"V2Persons", ";"))
                    .withColumn("Tone", udfTone($"V2Tone"))

  val df3GkgPersons = df32.select( $"SourceCommonName", $"Year", $"Month", $"Day", explode($"Persons_tmp"), $"Tone")

  val df3GkgCleanPersons = df3GkgPersons.select( "SourceCommonName", "Year", "Month", "Day", "col", "Tone")
                    .withColumn("Person", split($"col", ",")(0))
                    .drop("col", "V2Tones", "V2Persons", "Persons_tmp")

  val df3GkgSourceDistinctPersons = df3GkgCleanPersons.select( "SourceCommonName", "Year", "Month", "Day", "Tone", "Person").distinct().filter(!($"Person" === ""))

  val df3GkgSourceDistinctPersonsAvTone = df3GkgSourceDistinctPersons.select( "SourceCommonName", "Year", "Month", "Day", "Tone", "Person")
                              .groupBy( "SourceCommonName", "Year", "Month", "Day", "Person").agg(mean("Tone")).withColumn("AverageTone", udfAvTone($"avg(Tone)"))

  df3GkgSourceDistinctPersonsAvTone.show(30, false)*/

  /************* Dataframes to get Locations from articles with Date and Tone *************/
/*** Je garde le full name de la localisation, a voir si ce que l'on veut ***/
 /* val df33 = dfGkg.select("SourceCommonName", "Year", "Month", "Day", "V2Locations", "V2Tone")
    .filter(!($"SourceCommonName"=== ""))
    .withColumn("Type", lit("Locations"))
    .withColumn("Locations_tmp", split($"V2Locations", ";"))
    .withColumn("Tone", udfTone($"V2Tone"))

  val df3GkgLocations = df33.select( $"SourceCommonName", $"Year", $"Month", $"Day", explode($"Locations_tmp"), $"Tone")

  val df3GkgLocationsFullName = df3GkgLocations.select( "SourceCommonName", "Year", "Month", "Day", "col", "Tone")
    .withColumn("Location", split($"col", "#")(1))
    .drop("col", "V2Tones", "V2Locations", "Locations_tmp")

  val df3GkgDistinctLocationsFullName = df3GkgLocationsFullName.select( "SourceCommonName", "Year", "Month", "Day", "Tone", "Location").distinct().filter(!($"Location" === ""))

  val df3GkgDistinctLocationsFullNameAvTone = df3GkgDistinctLocationsFullName.select( "SourceCommonName", "Year", "Month", "Day", "Tone", "Location")
    .groupBy("SourceCommonName", "Year", "Month", "Day", "Location").agg(mean("Tone")).withColumn("AverageTone", udfAvTone($"avg(Tone)"))
    .sortWithinPartitions("SourceCommonName")

  df3GkgDistinctLocationsFullNameAvTone.show(30, false)
*/
  print(gkgRDD.count())

  println("END")

}