package paristech
import sys.process._
import java.net.URL
import java.io.File
import java.io.File
import java.nio.file.{ Files, StandardCopyOption }
import java.net.HttpURLConnection
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LoadingCSVFiles extends App {

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

  def fileDownloader(urlOfFileToDownload: String, fileName: String) = {
    print("Loading " + fileName + " ...")
    val url = new URL(urlOfFileToDownload)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(0)
    connection.setReadTimeout(0)
    connection.connect()

    if (connection.getResponseCode >= 400)
      println("error")
    else {
      println(" Done!")
      url #> new File(fileName) !!
    }
  }

  
  
  // Chargement du fichier "master"
  fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", "/tmp/masterfilelist.txt") // save the list file to the Spark Master

  // Chargement du fichier "master translation"
  fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt", "/tmp/masterfilelist-translation.txt") // save the list file to the Spark Master

  
  val filesDF = spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("/tmp/masterfilelist.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")

  val filesDF2 = filesDF.union(spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("/tmp/masterfilelist-translation.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")).cache()
    
  filesDF2.show(false)
  val sampleDF = filesDF2.filter(col("url").contains("/20190911")).cache

  sampleDF.select("url").repartition(100).foreach(r => {
    val URL = r.getAs[String](0)
    val fileName = r.getAs[String](0).split("/").last
    val dir = "/tmp/"
    val localFileName = dir + fileName
    fileDownloader(URL, localFileName)
    //val localFile = new File(localFileName)
    //AwsClient.s3.putObject("john-doe-telecom-gdelt2018", fileName, localFile )
    //localFile.delete()

  })

  println("Loading ends")
}
