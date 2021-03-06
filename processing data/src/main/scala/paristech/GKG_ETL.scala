package paristech
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader

import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode
import main.scala.paristech.AwsS3Client
import org.apache.spark.sql.SaveMode
import java.util.Calendar
import java.util.GregorianCalendar
import scala.collection.mutable.ListBuffer
import java.text.SimpleDateFormat
import main.scala.paristech.CassandraObject

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
    "spark.cassandra.connection.host" -> CassandraObject.IP))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("Event And Mention ETL")
    .getOrCreate()

  val AWS_ID = AwsS3Client.AWS_ID
  val AWS_KEY = AwsS3Client.AWS_KEY
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID) //(1) mettre votre ID du fichier credentials.csv
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY) //(2) mettre votre secret du fichier credentials.csv
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000") //(3) 15 par default !!!

  val log = Logger.getLogger("GKG_ETL")

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
    Day: Int)

  // On charge les fichiers Events
  def alldaysIn2019(): List[String] = {

    val dateFmt = "yyyyMMdd"
    val iYear = 2019
    val sdf = new SimpleDateFormat(dateFmt)
    var dates = new ListBuffer[String]()

    for (month <- Calendar.JANUARY to Calendar.DECEMBER) {
      // Create a calendar object and set year and month

      val mycal = new GregorianCalendar(iYear, month, 1);
      val daysInMonth = mycal.getActualMaximum(Calendar.DAY_OF_MONTH); // 28

      for (days <- 1 to daysInMonth) {
        val mycal = new GregorianCalendar(iYear, month, days);
        dates += sdf.format(mycal.getTime)
      }
    }

    return dates.toList
  }

  //val date = "20190101"
  
  alldaysIn2019().foreach(load)

  def load(date: String) = {
    
    log.info("Loading "+date)
    
    val gkgRDD = spark.sparkContext.binaryFiles("s3a://" + AwsS3Client.BUCKET + "/" + date + "*.gkg.csv.zip", 100).
      flatMap { // decompresser les fichiers
        case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile {
              case null =>
                zis.close(); false
              case _ => true
            }.flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
      }

    val cachedGkg = gkgRDD.cache // RDD

    def toInt(s: String): Int = if (s.isEmpty) 0 else s.toInt

    def toBigInt(s: String): BigInt = if (s.isEmpty) BigInt(0) else BigInt(s)

    def toYear(s: String): Int = if (s.isEmpty) 0 else (s.slice(0, 0 + 4)).toInt
    def toMonth(s: String): Int = if (s.isEmpty) 0 else (s.slice(4, 4 + 2)).toInt
    def toDay(s: String): Int = if (s.isEmpty) 0 else (s.slice(6, 6 + 2)).toInt

    val dfGkg = cachedGkg.map(_.split("\t")).filter(_.length == 27).map(
      e => Gkg(
        e(0), toBigInt(e(1)), toInt(e(2)), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15), e(16),
        e(17), e(18), e(19), e(20), e(21), e(22), e(23), e(24), e(25), e(26), toYear(e(1)), toMonth(e(1)), toDay(e(1)))).toDF.cache
/*********** UDF **************/

    def getTone(tones: String): Double = {
      try {
        tones.split(",")(0).toDouble
      } catch {
        case e: Exception => 0
      }
    } // keep only average Tone from GKG Tone field

    val udfTone = udf(getTone _)

/**** A revoir en fonction des moyennes obtenues sur plusieurs jours pour definir l'interval de Neutralite****/
    //def getAverageTone(tones: Double): String = if (tones < 0) "Neg" else "Pos"
    def getAverageTone(tones: Double): Double = tones

    val udfAvTone = udf(getAverageTone _)

/********* Dataframes to get Themes of articles with Date and Tone **********/

    val df3Gkg = dfGkg.select("DocumentIdentifier", "SourceCommonName", "Year", "Month", "Day", "V2Themes", "V2Tone")
      .filter(!($"SourceCommonName" === ""))
      //.groupBy("SourceCommonName", "V2Themes","Date", "V2Tone")
      //.withColumn("Type", lit("Themes")) Trois schemas disctincts pour Theme/Persons/Locations
      .withColumn("Theme_tmp", split($"V2Themes", ";"))
      .withColumn("tone", udfTone($"V2Tone")) // average tone of the article

    val df3GkgThemes = df3Gkg.select($"DocumentIdentifier", $"SourceCommonName", $"Year", $"Month", $"Day", explode($"Theme_tmp"), $"tone") //creation d'une ligne par theme pr chaque article

    val df3GkgCleanThemes = df3GkgThemes.select("DocumentIdentifier", "SourceCommonName", "Year", "Month", "Day", "col", "tone")
      .withColumn("theme", split($"col", ",")(0)) // Keep only Theme not it's location in the article
      .drop("col", "V2Tones", "V2Themes", "Theme_tmp")

/**** Verifier qu'on a bien un distinct que sur les themes lorsque l'on ajoutera d'autres jours de data ****/

    val df3GkgSourceDistinctThemes = df3GkgCleanThemes.dropDuplicates("DocumentIdentifier", "Theme").filter(!($"theme" === "")) //Suppression des doublons theme dans un meme article

/**** Idem Verifier le group by avec plusieurs jours, et surtout veut-on creer un groupby month and year aussi??****/
    val df3GkgSourceDistinctThemesAvTone = df3GkgSourceDistinctThemes.select("SourceCommonName", "Year", "Month", "Day", "tone", "theme")
      .groupBy("SourceCommonName", "Year", "Month", "Day", "theme").agg(mean("tone"), count(lit(1)).alias("count")).withColumn("AverageTone", udfAvTone($"avg(Tone)"))
    // .sortWithinPartitions("SourceCommonName")
    //print( df3GkgSourceDistinctThemesAvTone.filter($"SourceCommonName" === "scotcampus.com").filter($"Theme" === "MANMADE_DISASTER_IMPLIED").count())

    val df3GkgSourceDistinctThemesAvToneClean = df3GkgSourceDistinctThemesAvTone.withColumnRenamed("SourceCommonName", "source").withColumnRenamed("AverageTone", "tone")
      .withColumnRenamed("Year", "year").withColumnRenamed("Month", "month").withColumnRenamed("Day", "day")
      .drop("avg(tone)")

    //test nbipython
    // df3GkgSourceDistinctThemesAvTone.filter($"SourceCommonName" === "scotcampus.com").filter($"Theme" === "EDUCATION").show( false)
    // val test = df3GkgSourceDistinctThemesAvTone.filter('SourceCommonName' == "scotcampus.com")

    //df3GkgSourceDistinctThemesAvToneClean.show(20, false)

    // On enregistre l'aggrégation dans la table requete3

    df3GkgSourceDistinctThemesAvToneClean.write
      .cassandraFormat("req31", "nosql", "test")
      .mode(SaveMode.Append)
      .save()

    //nosql> CREATE TABLE req31(year int, month int, day int, source text, theme text, tone text, PRIMAREY KEY((source),year, month, day)) WITH CLUSTERING ORDER BY (year desc, month asc, day asc);

/************* Dataframes to get Persons from articles with Date and Tone *************/

    val df32 = dfGkg.select("SourceCommonName", "Year", "Month", "Day", "V2Persons", "V2Tone")
      .filter(!($"SourceCommonName" === ""))
      .withColumn("Type", lit("Persons"))
      .withColumn("Persons_tmp", split($"V2Persons", ";"))
      .withColumn("tone", udfTone($"V2Tone"))

    val df3GkgPersons = df32.select($"SourceCommonName", $"Year", $"Month", $"Day", explode($"Persons_tmp"), $"tone")

    val df3GkgCleanPersons = df3GkgPersons.select("SourceCommonName", "Year", "Month", "Day", "col", "tone")
      .withColumn("Person", split($"col", ",")(0))
      .drop("col", "V2Tones", "V2Persons", "Persons_tmp")

    val df3GkgSourceDistinctPersons = df3GkgCleanPersons.select("SourceCommonName", "Year", "Month", "Day", "Tone", "person").distinct().filter(!($"person" === ""))

    val df3GkgSourceDistinctPersonsAvTone = df3GkgSourceDistinctPersons.select("SourceCommonName", "Year", "Month", "Day", "tone", "person")
      .groupBy("SourceCommonName", "Year", "Month", "Day", "person").agg(mean("Tone"), count(lit(1)).alias("count")).withColumn("AverageTone", udfAvTone($"avg(tone)"))

    val df3GkgSourceDistinctPersonessAvToneClean = df3GkgSourceDistinctPersonsAvTone.withColumnRenamed("SourceCommonName", "source").withColumnRenamed("AverageTone", "tone")
      .withColumnRenamed("Year", "year").withColumnRenamed("Month", "month").withColumnRenamed("Day", "day")
      .drop("avg(tone)")

    // df3GkgSourceDistinctPersonsAvTone.show(30, false)
    df3GkgSourceDistinctPersonessAvToneClean.write
      .cassandraFormat("req32", "nosql", "test")
      .mode(SaveMode.Append)
      .save()

/************* Dataframes to get Locations from articles with Date and Tone *************/
/*** On regarde par pays de localisation***/
    val df33 = dfGkg.select("SourceCommonName", "Year", "Month", "Day", "V2Locations", "V2Tone")
      .filter(!($"SourceCommonName" === ""))
      .withColumn("Type", lit("Locations"))
      .withColumn("Locations_tmp", split($"V2Locations", ";"))
      .withColumn("tone", udfTone($"V2Tone"))

    val df3GkgLocations = df33.select($"SourceCommonName", $"Year", $"Month", $"Day", explode($"Locations_tmp"), $"tone")

    val df3GkgLocationsCountryName = df3GkgLocations.select("SourceCommonName", "Year", "Month", "Day", "col", "tone")
      .withColumn("location", split($"col", "#")(1)) // On garde le nom du pays
      .drop("col", "V2Tones", "V2Locations", "Locations_tmp")

    val df3GkgDistinctLocationsCountryName = df3GkgLocationsCountryName.select("SourceCommonName", "Year", "Month", "Day", "tone", "location").distinct().filter(!($"Location" === ""))

    val df3GkgDistinctLocationsCountryNameAvTone = df3GkgDistinctLocationsCountryName.select("SourceCommonName", "Year", "Month", "Day", "tone", "location")
      .groupBy("SourceCommonName", "Year", "Month", "Day", "location").agg(mean("Tone"), count(lit(1)).alias("count")).withColumn("AverageTone", udfAvTone($"avg(tone)"))
      .sortWithinPartitions("SourceCommonName")

    val df3GkgSourceDistinctLocalisationsAvToneClean = df3GkgDistinctLocationsCountryNameAvTone.withColumnRenamed("SourceCommonName", "source").withColumnRenamed("AverageTone", "tone")
      .withColumnRenamed("Year", "year").withColumnRenamed("Month", "month").withColumnRenamed("Day", "day")
      .drop("avg(tone)")

    // df3GkgDistinctLocationsFullNameAvTone.show(30, false)
    df3GkgSourceDistinctLocalisationsAvToneClean.write
      .cassandraFormat("req33", "nosql", "test")
      .mode(SaveMode.Append)
      .save()

    dfGkg.unpersist()
    cachedGkg.unpersist()
    log.info("End loading "+date)
  }

}