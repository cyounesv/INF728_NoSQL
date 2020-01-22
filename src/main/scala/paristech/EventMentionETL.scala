package paristech
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import main.scala.paristech.AwsS3Client
import org.apache.spark.sql.SaveMode
import java.util.Calendar
import java.util.GregorianCalendar
import scala.collection.mutable.ListBuffer
import java.text.SimpleDateFormat
import main.scala.paristech.CassandraObject

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
    "spark.cassandra.connection.host"->CassandraObject.IP))

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

  alldaysIn2019().foreach { load(_) }
  
  def load(date: String) = {

    // Code du prof pour faire ca

    val eventsRDD = spark.sparkContext.binaryFiles("s3a://" + AwsS3Client.BUCKET + "/" + date + "*.export.CSV.zip", 100).
      flatMap { // decompresser les fichiers
        case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile{ case null => zis.close(); false
            case _ => true }.
            flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }          
      }
    val cachedEvents = eventsRDD.cache // RDD

    def toDouble(s: String): Double = try { if (s.isEmpty) 0 else s.toDouble } catch {case e: Exception => 0}
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
            takeWhile{ case null => zis.close(); false
            case _ => true }.
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
  }

}