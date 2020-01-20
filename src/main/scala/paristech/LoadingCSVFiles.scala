package paristech
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import scala.reflect.io.Directory
import java.io.File

import scala.sys.process._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}





class LoadingCSVFiles(spark: SparkSession) extends Serializable {
  
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("LoadingCSVFiles")
  
  log.setLevel(Level.WARN) 

  private def fileDownloader(urlOfFileToDownload: String, fileName: String) = {
    log.warn("Loading " + fileName + " ...")
    val url = new URL(urlOfFileToDownload)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(0)
    connection.setReadTimeout(0)
    connection.connect()

    if (connection.getResponseCode >= 400)
      log.error("error")
    else {
      log.warn(" Done!")
      url #> new File(fileName) !!
    }
  }

  // Test de l'existance des fichiers masters
  
  
    
    @transient
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    hdfs.mkdirs(new Path("/mnt/tmp/"))
    
    

  if (!new File("/tmp/masterfilelist.txt").exists) {

    var localFileName = "/mnt/tmp/masterfilelist.txt"
    var fileName = "masterfilelist.txt"
    
    // Chargement du fichier "master"
    fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", "/mnt/tmp/masterfilelist.txt") // save the list file to the Spark Master
    hdfs.copyFromLocalFile(new Path(localFileName), new Path("/mnt/tmp/"+fileName))  
 
    localFileName = "/mnt/tmp/masterfilelist-translation.txt"
    fileName = "masterfilelist-translation.txt"
    
    // Chargement du fichier "master translation"
    fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt", "/mnt/tmp/masterfilelist-translation.txt") // save the list file to the Spark Master   
    hdfs.copyFromLocalFile(new Path(localFileName), new Path("/mnt/tmp/"+fileName))
  }

  private val filesDF = spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("/mnt/tmp/masterfilelist.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")

  private val filesDF2 = filesDF.union(spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("/mnt/tmp/masterfilelist-translation.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")).cache()

  def deleteEventOneDay(day: String) = {
    val directory = new Directory(new File("/mnt/tmp/" + day))
    directory.files.foreach(f => {
      if (f.name.endsWith("export.CSV.zip") || f.name.endsWith(("mentions.CSV.zip")))
        f.delete()
    })
  }

  def deleteOneDay(day: String) = {
    val directory = new Directory(new File("/mnt/tmp/" + day))
    directory.deleteRecursively()
  }

  def loadEventOneDay(day: String, overwrite: Boolean = false): String = {
    val filters = List("export", "mentions");
    loadOneDay(filters, day, overwrite)
  }

  private def loadOneDay(filters: List[String], day: String, overwrite: Boolean = false): String = {

    val sampleDF = filesDF2.filter(col("url").contains("/" + day))
    val toReturn = "/mnt/tmp/" + day;

    val directory = new Directory(new File("/mnt/tmp/" + day))
    if (directory.exists) {
      if (overwrite) {
        deleteOneDay(day)
        directory.createDirectory(false, true)
      } else {
        return toReturn
      }
    } else {
      directory.createDirectory(false, true)
    }
    
    
    hdfs.mkdirs(new Path("/mnt/tmp/"+day))
   
   import spark.implicits._
   
    sampleDF.select("url").collect().foreach(r => {
      val URL = r.getAs[String](0)
      val fileName = r.getAs[String](0).split("/").last
      //      var ok = false
      //
      //      filters.foreach(f => ok |= fileName.contains(f))
      //
      //      if (ok) {
      val dir = "/mnt/tmp/" + day + "/"
      val localFileName = dir + fileName
      fileDownloader(URL, localFileName)   
      hdfs.copyFromLocalFile(new Path(localFileName), new Path(localFileName))
      
      //      }
    })
    

    return toReturn
  }
}
