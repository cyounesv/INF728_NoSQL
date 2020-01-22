package paristech
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.{ CassandraConnector, CassandraConnectorConf }
import com.datastax.spark.connector.rdd.ReadConf
import main.scala.paristech.AwsS3Client
import org.apache.spark.sql.SaveMode
import java.util.Calendar
import java.util.GregorianCalendar
import scala.collection.mutable.ListBuffer
import java.text.SimpleDateFormat
import main.scala.paristech.CassandraObject
import org.apache.spark.sql.functions.udf
import com.datastax.spark.connector._
import org.apache.spark.sql.types.IntegerType

object EventMentionETL extends App {

  Logger.getLogger("akka").setLevel(Level.WARN)
  val log = Logger.getLogger("EVENT MENTION ETL")

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

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  /**
   * Je suppose le chargement dans le repertoire /tmp
   *
   *
   */

  // D.E Evenement
  case class Event(
    GLOBALEVENTID: Int,
    SQLDATE: Int,
    MonthYear: Int,
    Year: Int,
    FractionDate: Double,
    Actor1Code: String,
    Actor1Name: String,
    Actor1CountryCode: String,
    Actor1KnownGroupCode: String,
    Actor1EthnicCode: String,
    Actor1Religion1Code: String,
    Actor1Religion2Code: String,
    Actor1Type1Code: String,
    Actor1Type2Code: String,
    Actor1Type3Code: String,
    Actor2Code: String,
    Actor2Name: String,
    Actor2CountryCode: String,
    Actor2KnownGroupCode: String,
    Actor2EthnicCode: String,
    Actor2Religion1Code: String,
    Actor2Religion2Code: String,
    Actor2Type1Code: String,
    Actor2Type2Code: String,
    Actor2Type3Code: String,
    IsRootEvent: Int,
    EventCode: String,
    EventBaseCode: String,
    EventRootCode: String,
    QuadClass: Int,
    GoldsteinScale: Double,
    NumMentions: Int,
    NumSources: Int,
    NumArticles: Int,
    AvgTone: Double,
    Actor1Geo_Type: Int,
    Actor1Geo_FullName: String,
    Actor1Geo_CountryCode: String,
    Actor1Geo_ADM1Code: String,
    Actor1Geo_ADM2Code: String,
    Actor1Geo_Lat: Double,
    Actor1Geo_Long: Double,
    Actor1Geo_FeatureID: String,
    Actor2Geo_Type: Int,
    Actor2Geo_FullName: String,
    Actor2Geo_CountryCode: String,
    Actor2Geo_ADM1Code: String,
    Actor2Geo_ADM2Code: String,
    Actor2Geo_Lat: Double,
    Actor2Geo_Long: Double,
    Actor2Geo_FeatureID: String,
    ActionGeo_Type: Int,
    ActionGeo_FullName: String,
    ActionGeo_CountryCode: String,
    ActionGeo_ADM1Code: String,
    ActionGeo_ADM2Code: String,
    ActionGeo_Lat: Double,
    ActionGeo_Long: Double,
    ActionGeo_FeatureID: String,
    DATEADDED: BigInt,
    SOURCEURL: String,
    Month: Int,
    Day: Int)

  case class EventMention(
    GLOBALEVENTID: Int,
    EventTimeDate: BigInt,
    MentionTimeDate: BigInt,
    MentionType: Int,
    MentionSourceName: String,
    MentionIdentifier: String,
    SentenceID: Int,
    Actor1CharOffset: Int,
    Actor2CharOffset: Int,
    ActionCharOffset: Int,
    InRawText: Int,
    Confidence: Int,
    MentionDocLen: Int,
    MentionDocTone: Double,
    MentionDocTranslationInfo: String // NULL SI ENG
  // Extras: String // Non utilise
  )

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

  alldaysIn2019().slice(0, 3).foreach { load(_) }

  def load(date: String) = {

    // Code du prof pour faire ca

    val eventsRDD = spark.sparkContext.binaryFiles("s3a://" + AwsS3Client.BUCKET + "/" + date + "*.export.CSV.zip", 100).
      flatMap { // decompresser les fichiers
        case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile {
              case null =>
                zis.close(); false
              case _ => true
            }.
            flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
      }
    val cachedEvents = eventsRDD.cache // RDD

    def toDouble(s: String): Double = try { if (s.isEmpty) 0 else s.toDouble } catch { case e: Exception => 0 }
    def toInt(s: String): Int = if (s.isEmpty) 0 else s.toInt
    def toBigInt(s: String): BigInt = if (s.isEmpty) BigInt(0) else BigInt(s)
    def toYear(s: String): Int = if (s.isEmpty) 0 else (s.slice(0, 0 + 4)).toInt
    def toMonth(s: String): Int = if (s.isEmpty) 0 else (s.slice(4, 4 + 2)).toInt
    def toDay(s: String): Int = if (s.isEmpty) 0 else (s.slice(6, 6 + 2)).toInt

    val dfEvent = cachedEvents.map(_.split("\t")).filter(_.length == 61).map(
      e => Event(
        toInt(e(0)), toInt(e(1)), toInt(e(2)), toInt(e(3)), toDouble(e(4)), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15), e(16), e(17), e(18), e(19), e(20),
        e(21), e(22), e(23), e(24), toInt(e(25)), e(26), e(27), e(28), toInt(e(29)), toDouble(e(30)), toInt(e(31)), toInt(e(32)), toInt(e(33)), toDouble(e(34)), toInt(e(35)), e(36), e(37), e(38), e(39), toDouble(e(40)),
        toDouble(e(41)), e(42), toInt(e(43)), e(44), e(45), e(46), e(47), toDouble(e(48)), toDouble(e(49)), e(50), toInt(e(51)), e(52), e(53), e(54), e(55), toDouble(e(56)), toDouble(e(57)), e(58), toBigInt(e(59)), e(60), toMonth(e(1)), toDay(e(1)))).toDF.cache

    // On charge les fichiers Mention
    // Code du prof pour faire ca

    val mentionsRDD = spark.sparkContext.binaryFiles("s3a://" + AwsS3Client.BUCKET + "/" + date + "*.mentions.CSV.zip", 100).
      flatMap { // decompresser les fichiers
        case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile {
              case null =>
                zis.close(); false
              case _ => true
            }.
            flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
      }
    val mentionsEvents = mentionsRDD.cache // RDD

    // Si je ne trouve pas d'info de translate, j'utilise "Eng"
    val dfMention = mentionsEvents.map(_.split("\t")).filter(_.length >= 14).map(
      e => if (e.length == 14) {
        EventMention(
          toInt(e(0)), toBigInt(e(1)), toBigInt(e(2)), toInt(e(3)), e(4), e(5), toInt(e(6)), toInt(e(7)), toInt(e(8)), toInt(e(9)), toInt(e(10)), toInt(e(11)), toInt(e(12)), toDouble(e(13)), "eng")
      } else {
        EventMention(
          toInt(e(0)), toBigInt(e(1)), toBigInt(e(2)), toInt(e(3)), e(4), e(5), toInt(e(6)), toInt(e(7)), toInt(e(8)), toInt(e(9)), toInt(e(10)), toInt(e(11)), toInt(e(12)), toDouble(e(13)), e(14).split(';')(0).split(':')(1))
      }).toDF.cache
    // Requette 1
    // il faut faire du casandra mais ca ne marche pas dans mon docker; a revoir dans AWS
    // Schema : Date, Pays, Langue
    // Tout le nommage est à revoir
    // Je considere qu'on a parser le champ "MentionDocTranslationInfo" de la BDD pour avoir qqchose de propre
    // J'ai pris les colonnes YEAR et MOUNTH pour faire le COUNT dessus pour apres

    val eventLanguage = dfMention.select("GLOBALEVENTID", "MentionDocTranslationInfo").distinct

    val cassandra1 = dfEvent.join(eventLanguage, Seq("GLOBALEVENTID")).select("SQLDATE", "ActionGeo_CountryCode", "MentionDocTranslationInfo").groupBy("SQLDATE", "ActionGeo_CountryCode", "MentionDocTranslationInfo").count()

    // Pour verifier la jointure
    //val cassandra2 = dfEvent.select("GLOBALEVENTID", "SQLDATE", "ActionGeo_CountryCode", "Year", "MonthYear").groupBy("Year", "MonthYear", "SQLDATE", "ActionGeo_CountryCode").count().sort($"count".desc)

    // On recherche la langue des events qui sont dans les mentions

    //cassandra1.show()
    //cassandra2.show()

    val cassandraToSave = cassandra1.withColumnRenamed("SQLDATE", "jour").withColumnRenamed("ActionGeo_CountryCode", "pays").withColumnRenamed("MentionDocTranslationInfo", "langue")

    cassandraToSave.write.mode(SaveMode.Append)
      .cassandraFormat("requete1", "nosql", "test")
      .save()

    // Requete 2
    // On collecte les infos de la table mention

    //dfMention.printSchema()
    def toDate(date: java.math.BigDecimal): Int = {
      date.toString().take(8).toInt
    }
    val dateTimeToDate = udf(toDate _)

    val dfMentionUpdated = dfMention.select("GLOBALEVENTID", "EventTimeDate", "MentionTimeDate", "MentionDocTone")
      .withColumn("EventDate", dateTimeToDate(col("EventTimeDate")))
      .filter($"EventDate" > 20190000)
      .withColumn("MentionDate", dateTimeToDate(col("MentionTimeDate")))

    // On récupère les mentions dont la date est différente de celle de l'évenement (elles sont déjà dans cassandra)
    val dfMentionAlreadyInDb = dfMentionUpdated.filter(col("EventDate") =!= col("MentionDate"))

    //Pour tester lorsqu'un event des jours passés survient: (penser à le supprimer dans cassnadra avant):
    //val dfMentionAlreadyInDb = dfMentionUpdated.filter(col("GLOBALEVENTID") === 890019084 )

    // on récupère les autres à enregistrer
    //val dfMentionFiltered = dfMention.filter(col("EventDate") === col("MentionDate") )
    val numMentions = dfMentionUpdated
      .filter(col("EventDate") === col("MentionDate"))
      .select("GLOBALEVENTID", "MentionDocTone")
      .groupBy("GLOBALEVENTID").agg(sum("MentionDocTone"), count("GLOBALEVENTID"))

    //numMentions.show()

    val requete2 = dfEvent.join(numMentions, Seq("GLOBALEVENTID")).select("GLOBALEVENTID", "SQLDATE", "ActionGeo_CountryCode", "Year", "MonthYear", "count(GLOBALEVENTID)", "Actor1Geo_CountryCode", "Actor2Geo_CountryCode", "sum(MentionDocTone)", "Actor1Geo_Lat", "Actor2Geo_Lat", "Actor1Geo_Long", "Actor2Geo_Long")
      .withColumn("ActionGeo_CountryCode", when($"ActionGeo_CountryCode" === "", "unknown")
        .otherwise($"ActionGeo_CountryCode"))

    // event mentionned on several dates : id = 871834248

    val requete2Renamed = requete2.withColumnRenamed("SQLDATE", "day")
      .withColumnRenamed("ActionGeo_CountryCode", "country")
      .withColumnRenamed("GLOBALEVENTID", "eventid")
      .withColumnRenamed("Year", "year")
      .withColumnRenamed("MonthYear", "monthyear")
      .withColumnRenamed("Actor1Geo_CountryCode", "actor1countrycode")
      .withColumnRenamed("Actor2Geo_CountryCode", "actor2countrycode")
      .withColumnRenamed("Actor1Geo_Lat", "actor1lat")
      .withColumnRenamed("Actor2Geo_Lat", "actor2lat")
      .withColumnRenamed("Actor1Geo_Long", "actor1long")
      .withColumnRenamed("Actor2Geo_Long", "actor2long")
      .withColumnRenamed("sum(MentionDocTone)", "sumtone")
      .withColumnRenamed("count(GLOBALEVENTID)", "count")

    val requete2ToSave = requete2Renamed.select("eventid", "country", "year", "monthyear", "day", "count")
    val request2Mapping = requete2Renamed.select("eventid", "country", "day", "actor1countrycode", "actor2countrycode", "actor1lat", "actor2lat", "actor1long", "actor2long", "sumtone", "count")

    // On enregistre l'aggrégation dans la table requete2 et on sauveegarde l'association eventid/pays dans la table requete2mapping

    requete2ToSave.write
      .cassandraFormat("requete2", "nosql", "test")
      .mode(SaveMode.Append)
      .save()

    request2Mapping.write
      .cassandraFormat("requete2mapping", "nosql", "test")
      .mode(SaveMode.Append)
      .save()

    // Si on a des eventid qui sont déjà présents dans cassandra:
    if (!dfMentionAlreadyInDb.isEmpty) {

      // On fait l'aggrégation du nombre de mentions
      val dfMentionAlreadyInDbAgg = dfMentionAlreadyInDb.select("GLOBALEVENTID", "EventDate", "MentionDocTone")
        .groupBy("GLOBALEVENTID", "EventDate")
        .agg(sum("MentionDocTone"), count("GLOBALEVENTID"))
        .withColumnRenamed("sum(MentionDocTone)", "sumtoneNew")
        .withColumnRenamed("count(GLOBALEVENTID)", "countNew")

      // udf qui permet de requeter la table requete2mapping afin d'avoir le pays et la date de l'event à partir de son id
      def getFields(eventid: String): Array[String] = {
        val table = spark.sparkContext.cassandraTable("nosql", "requete2mapping")
        val getCountryDayTable = table
          .select("country", "day", "count", "sumtone", "actor1countrycode", "actor2countrycode", "actor1lat", "actor2lat", "actor1long", "actor2long")
          .where("eventid='" + eventid + "'")
        if (getCountryDayTable.isEmpty) {
          log.error("Error, requete2mapping does not contain eventid: " + eventid)
          Array("", "", "", "","", "", "", "","", "")
        } else {
          val country = getCountryDayTable.first().getString("country")
          val day = getCountryDayTable.first().getString("day")
          val count = getCountryDayTable.first().getString("count")
          val sumtone = getCountryDayTable.first().getString("sumtone")
          val actor1countrycode = getCountryDayTable.first().getString("actor1countrycode")
          val actor2countrycode = getCountryDayTable.first().getString("actor2countrycode")
          val actor1lat = getCountryDayTable.first().getString("actor1lat")
          val actor2lat = getCountryDayTable.first().getString("actor2lat")
          val actor1long = getCountryDayTable.first().getString("actor1long")
          val actor2long = getCountryDayTable.first().getString("actor2long")          
          Array(country, day, count, sumtone,actor1countrycode, actor2countrycode, actor1lat, actor2lat, actor1long, actor2long)
        }

      }
      val getFieldsCol = udf(getFields _)

      // udf qui permet d'aller chercher le nombre de mention dans la table requête 2
      /*def getCount(country: String, year:Int, monthyear:Int, day:Int, count: Int, eventid:String): Int= {
    val getCountTable = spark.sparkContext.cassandraTable("nosql", "requete2")
      .select("count")
      .where("country='" + country + "'")
      .where("year=" + year.toString())
      .where("monthyear=" + monthyear.toString())
      .where("day=" + day.toString())
      .where("eventid='" + eventid + "'")

    if (getCountTable.count() == 0) {
      println("Error, requete2 does not contain eventid: " + eventid)
      0
    } else {
      getCountTable.first().getInt("count") + count
    }
  }
   */

      // udf pour déterminer year et monthyear à partir de la date de la mention
      def toMonthYear(date: String, size: Int) = {
        //date.toString().take(size).toInt
        date.take(size)
      }

      val date = udf(toMonthYear _)
      
      

      val dfMentionAlreadyInDbUpdated = dfMentionAlreadyInDbAgg.withColumn("newCol", getFieldsCol(col("GLOBALEVENTID")))
        .select($"GLOBALEVENTID", $"countNew", $"newCol"(0).as("country"), $"newCol"(1).as("day"),
          $"newCol"(2).as("countOr").cast(IntegerType), $"newCol"(3).as("sumtoneOr").cast(IntegerType),
          $"newCol"(4).as("actor1countrycode"), $"newCol"(5).as("actor2countrycode"),
          $"newCol"(6).as("actor1lat"), $"newCol"(7).as("actor2lat"),
          $"newCol"(8).as("actor1long"), $"newCol"(9).as("actor2long"),
          $"sumtoneNew")
        .filter(col("country").isNotNull)
        .withColumn("monthyear", date(col("day"), lit(6)))
        .withColumn("year", date(col("day"), lit(4)))
        .withColumnRenamed("GLOBALEVENTID", "eventid")
        .withColumn("count", $"countOr" + $"countNew")
        .withColumn("sumtone", $"sumtoneOr" + $"sumtoneNew")
        .drop("sumtoneOr").drop("sumtoneNew").drop("countOr").drop("countNew")

      //dfMentionAlreadyInDbUpdated.show()

      //val startTimeMillis = System.currentTimeMillis()

      if (!dfMentionAlreadyInDbUpdated.isEmpty) {
        val dfMentionAlreadyInDbUpdatedRequest2 = dfMentionAlreadyInDbUpdated.select("country", "monthyear", "year", "eventid", "count", "day")

        dfMentionAlreadyInDbUpdatedRequest2.write
          .cassandraFormat("requete2", "nosql", "test")
          .mode(SaveMode.Append)
          .option("confirm.truncate", "true")
          .save()

        val dfMentionAlreadyInDbUpdatedMapping = dfMentionAlreadyInDbUpdated.drop("monthyear", "year")

        dfMentionAlreadyInDbUpdatedMapping.write
          .cassandraFormat("requete2mapping", "nosql", "test")
          .mode(SaveMode.Append)
          .option("confirm.truncate", "true")
          .save()

      }
      //val endTimeMillis = System.currentTimeMillis()
      //val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      //println(durationSeconds)
    }
  }

  println("end")
}