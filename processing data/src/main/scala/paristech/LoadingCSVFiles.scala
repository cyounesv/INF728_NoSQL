package paristech
import java.io.File
import java.net.HttpURLConnection
import java.net.URL

import scala.sys.process._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import main.scala.paristech.AwsS3Client



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
    "spark.driver.maxResultSize" -> "2g"))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("TP Spark : Trainer")
    .getOrCreate()
    
  val AWS_ID = AwsS3Client.AWS_ID
  val AWS_KEY = AwsS3Client.AWS_KEY
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID) //(1) mettre votre ID du fichier credentials.csv
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY) //(2) mettre votre secret du fichier credentials.csv
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000") //(3) 15 par default !!!

  
  spark.sparkContext.setLogLevel("WARN")

  // la classe AmazonS3Client n'est pas serializable
  // on rajoute l'annotation @transient pour dire a Spark de ne pas essayer de serialiser cette classe et l'envoyer aux executeurs
  /*@transient
  val awsClient = new AmazonS3Client(new BasicAWSCredentials(AWS_ID, AWS_KEY))
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID) // mettre votre ID du fichier credentials.csv
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY) // mettre votre secret du fichier credentials.csv
  */
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
  AwsS3Client.s3.putObject(AwsS3Client.BUCKET, "masterfilelist.txt", new File("/tmp/masterfilelist.txt"))

  // Chargement du fichier "master translation"
  fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt", "/tmp/masterfilelist-translation.txt") // save the list file to the Spark Master
  AwsS3Client.s3.putObject(AwsS3Client.BUCKET, "masterfilelist-translation.txt", new File("/tmp/masterfilelist-translation.txt"))

  val filesDF = spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("s3a://" + AwsS3Client.BUCKET + "/masterfilelist.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")

  val filesDF2 = filesDF.union(spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("s3a://" + AwsS3Client.BUCKET + "/masterfilelist-translation.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")).cache()

  filesDF2.show(false)
  val sampleDF = filesDF2.filter(col("url").contains("/2019")).cache

  sampleDF.select("url").repartition(100).foreach(r => {
    val URL = r.getAs[String](0)
    val fileName = r.getAs[String](0).split("/").last
    val dir = "/tmp/"
    val localFileName = dir + fileName
    fileDownloader(URL, localFileName)
    val localFile = new File(localFileName)
    AwsS3Client.s3.putObject(AwsS3Client.BUCKET, fileName, localFile)
    localFile.delete()

  })

  println("Loading ends")
}
