package paristech
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.functions.udf
import com.datastax.spark.connector._



object EventMentionETL extends App {

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
    "spark.cassandra.connection.connections_per_executor_max" -> "2"
  ))

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
    SOURCEURL: String)

  // On charge les fichiers Events
  // Code du prof pour faire ca

  val eventsRDD = spark.sparkContext.binaryFiles("/tmp/2019*.export.CSV.zip", 100).
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
  val cachedEvents = eventsRDD.cache // RDD

  def toDouble(s: String): Double = if (s.isEmpty) 0 else s.toDouble
  def toInt(s: String): Int = if (s.isEmpty) 0 else s.toInt
  def toBigInt(s: String): BigInt = if (s.isEmpty) BigInt(0) else BigInt(s)

  val dfEvent = cachedEvents.map(_.split("\t")).filter(_.length == 61).map(
    e => Event(
      toInt(e(0)), toInt(e(1)), toInt(e(2)), toInt(e(3)), toDouble(e(4)), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15), e(16), e(17), e(18), e(19), e(20),
      e(21), e(22), e(23), e(24), toInt(e(25)), e(26), e(27), e(28), toInt(e(29)), toDouble(e(30)), toInt(e(31)), toInt(e(32)), toInt(e(33)), toDouble(e(34)), toInt(e(35)), e(36), e(37), e(38), e(39), toDouble(e(40)),
      toDouble(e(41)), e(42), toInt(e(43)), e(44), e(45), e(46), e(47), toDouble(e(48)), toDouble(e(49)), e(50), toInt(e(51)), e(52), e(53), e(54), e(55), toDouble(e(56)), toDouble(e(57)), e(58), toBigInt(e(59)), e(60))).toDF.cache

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

  // On charge les fichiers Mention
  // Code du prof pour faire ca

  val mentionsRDD = spark.sparkContext.binaryFiles("/tmp/2019*.mentions.CSV.zip", 100).
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
  val mentionsEvents = mentionsRDD.cache // RDD
 

  // Si je ne trouve pas d'info de translate, j'utilise "Eng"
  val dfMention = mentionsEvents.map(_.split("\t")).filter(_.length >= 14).map(
    e => if (e.length == 14) EventMention(
      toInt(e(0)), toBigInt(e(1)), toBigInt(e(2)), toInt(e(3)), e(4), e(5), toInt(e(6)), toInt(e(7)), toInt(e(8)), toInt(e(9)), toInt(e(10)), toInt(e(11)), toInt(e(12)), toDouble(e(13)), "eng")
    else EventMention(
      toInt(e(0)), toBigInt(e(1)), toBigInt(e(2)), toInt(e(3)), e(4), e(5), toInt(e(6)), toInt(e(7)), toInt(e(8)), toInt(e(9)), toInt(e(10)), toInt(e(11)), toInt(e(12)), toDouble(e(13)), e(14).split(';')(0).split(':')(1) )).toDF.cache

  // Requete 1
  // il faut faire du casandra mais ca ne marche pas dans mon docker; a revoir dans AWS
  // Schema : Date, Pays, Langue
  // Tout le nommage est à revoir
  // Je considere qu'on a parser le champ "MentionDocTranslationInfo" de la BDD pour avoir qqchose de propre
  // J'ai pris les colonnes YEAR et MOUNTH pour faire le COUNT dessus pour apres
/*
  val eventLanguage = dfMention.select("GLOBALEVENTID", "MentionDocTranslationInfo").distinct

  val cassandra1 = dfEvent.join(eventLanguage, Seq("GLOBALEVENTID")).select("SQLDATE", "ActionGeo_CountryCode", "MentionDocTranslationInfo").groupBy("SQLDATE", "ActionGeo_CountryCode", "MentionDocTranslationInfo").count()

  
  // Pour verifier la jointure
  val cassandra2 = dfEvent.select("GLOBALEVENTID", "SQLDATE", "ActionGeo_CountryCode", "Year", "MonthYear").groupBy("Year", "MonthYear", "SQLDATE", "ActionGeo_CountryCode").count().sort($"count".desc)

  // On recherche la langue des events qui sont dans les mentions

  cassandra1.show()
  cassandra2.show()
  
  val cassandraToSave = cassandra1.withColumnRenamed("SQLDATE","jour").withColumnRenamed("ActionGeo_CountryCode","pays").withColumnRenamed("MentionDocTranslationInfo","langue")
  
  spark.setCassandraConf("Test", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1"))

  val createDDL = """CREATE TEMPORARY VIEW words
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "requete1",
     keyspace "nosql",
     cluster "test",
     pushdown "true")"""
spark.sql(createDDL) // Creates Catalog Entry registering an existing Cassandra Table


  cassandraToSave.write
  .cassandraFormat("requete1", "nosql", "test")
  .save()
*/
  // Requete 2

  // On collecte les infos de la table mention


  dfMention.printSchema()
  def toDate(date: java.math.BigDecimal) ={
     date.toString().take(8).toInt
  }
  val dateTimeToDate = udf(toDate _)

  val dfMentionUpdated = dfMention.select("GLOBALEVENTID", "EventTimeDate", "MentionTimeDate")
    .withColumn("EventDate", dateTimeToDate(col("EventTimeDate")))
    .withColumn("MentionDate", dateTimeToDate(col("MentionTimeDate")))

  // On récupère les mentions dont la date est différente de celle de l'évenement (elles sont déjà dans cassandra)
  //val dfMentionAlreadyInDb = dfMentionUpdated.filter(col("EventDate") =!= col("MentionDate") )

  // Pour tester lorsqu'un event des jours passés survient: (penser à le supprimer dans cassnadra avant):
  val dfMentionAlreadyInDb = dfMentionUpdated.filter(col("GLOBALEVENTID") === 890122482 )
  dfMentionAlreadyInDb.show()

  // on récupère les autres à enregistrer
  //val dfMentionFiltered = dfMention.filter(col("EventDate") === col("MentionDate") )
  val numMentions = dfMentionUpdated
    .filter(col("EventDate") === col("MentionDate") )
    .select("GLOBALEVENTID")
    .groupBy("GLOBALEVENTID")
    .count()

  val requete2 = dfEvent.join(numMentions, Seq("GLOBALEVENTID")).select("GLOBALEVENTID", "SQLDATE","ActionGeo_CountryCode","Year","MonthYear", "count")
    .withColumn("ActionGeo_CountryCode", when($"ActionGeo_CountryCode"==="", "unknown")
      .otherwise($"ActionGeo_CountryCode")
    ).sort($"count".desc)

  // event mentionned on several dates : id = 871834248

  val requete2ToSave = requete2.withColumnRenamed("SQLDATE","day")
    .withColumnRenamed("ActionGeo_CountryCode","country")
    .withColumnRenamed("GLOBALEVENTID","eventid")
    .withColumnRenamed("Year","year")
    .withColumnRenamed("MonthYear","monthyear")
  requete2ToSave.show()

  val request2Mapping = requete2ToSave.select("eventid", "country")


  spark.setCassandraConf("Test", CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1"))

// On enregistre l'aggrégation dans la table requete2 et on sauveegarde l'association eventid/pays dans la table requete2mapping
 /*
  requete2ToSave.write
   .cassandraFormat("requete2", "nosql", "test")
   .mode(SaveMode.Append)
   .save()

 request2Mapping.write
   .cassandraFormat("requete2mapping", "nosql", "test")
   .mode(SaveMode.Append)
   .save()
*/
  // Si on a des eventid qui sont déjà présents dans cassandra:
if(dfMentionAlreadyInDb.count() !=0) {

  // On fait l'aggrégation du nombre de mentions
  val dfMentionAlreadyInDbCount = dfMentionAlreadyInDb.select("GLOBALEVENTID", "MentionDate")
    .groupBy("GLOBALEVENTID", "MentionDate")
    .count()

  // udf pour aller chercher le pays de l'eventid dans cassandra
 def getCountry(eventid: String) = {
   val test1 = spark.sparkContext.cassandraTable("nosql", "requete2mapping")
     .select("country")
     .where("eventid='" + eventid + "'")
   if (test1.count() == 0) {
     null
   }else{
     test1.first().getString("country")
   }
 }

 val getCountryCol = udf(getCountry _)

  // udf pour déterminer year et monthyear à partir de la date de la mention
 def toMonthYear(date: Int, size: Int) = {
   date.toString().take(size).toInt
 }

 val date = udf(toMonthYear _)

 val dfMentionAlreadyInDbUpdated = dfMentionAlreadyInDbCount.withColumn("country", getCountryCol(col("GLOBALEVENTID")))
   .filter(col("country").isNotNull)
   .withColumn("monthyear", date(col("MentionDate"), lit(6)))
   .withColumn("year", date(col("MentionDate"), lit(4)))
   .withColumnRenamed("GLOBALEVENTID", "eventid")
   .withColumnRenamed("MentionDate", "day")

  dfMentionAlreadyInDbUpdated.show()
  if (dfMentionAlreadyInDbUpdated.count() != 0){
    dfMentionAlreadyInDbUpdated.write
      .cassandraFormat("requete2", "nosql", "test")
      .mode(SaveMode.Append)
      .save()
  }
}
 println("end")

}