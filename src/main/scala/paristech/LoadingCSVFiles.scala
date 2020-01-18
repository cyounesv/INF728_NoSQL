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

class LoadingCSVFiles(spark: SparkSession) extends Serializable {

  private def fileDownloader(urlOfFileToDownload: String, fileName: String) = {
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

  // Test de l'existance des fichiers masters

  if (!new File("/tmp/masterfilelist.txt").exists) {

    // Chargement du fichier "master"
    fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", "/tmp/masterfilelist.txt") // save the list file to the Spark Master
    // Chargement du fichier "master translation"
    fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt", "/tmp/masterfilelist-translation.txt") // save the list file to the Spark Master
  }

  private val filesDF = spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("/tmp/masterfilelist.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")

  private val filesDF2 = filesDF.union(spark.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv("/tmp/masterfilelist-translation.txt").
    withColumnRenamed("_c0", "size").
    withColumnRenamed("_c1", "hash").
    withColumnRenamed("_c2", "url")).cache()

  def deleteEventOneDay(day: String) = {
    val directory = new Directory(new File("/tmp/" + day))
    directory.files.foreach(f => {
      if (f.name.endsWith("export.CSV.zip") || f.name.endsWith(("mentions.CSV.zip")))
        f.delete()
    })
  }

  def deleteOneDay(day: String) = {
    val directory = new Directory(new File("/tmp/" + day))
    directory.deleteRecursively()
  }

  def loadEventOneDay(day: String, overwrite: Boolean = false): String = {
    val filters = List("export", "mentions");
    loadOneDay(filters, day, overwrite)
  }

  private def loadOneDay(filters: List[String], day: String, overwrite: Boolean = false): String = {

    val sampleDF = filesDF2.filter(col("url").contains("/" + day))
    val toReturn = "/tmp/" + day;

    val directory = new Directory(new File("/tmp/" + day))
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

    sampleDF.select("url").repartition(100).foreach(r => {
      val URL = r.getAs[String](0)
      val fileName = r.getAs[String](0).split("/").last
      var ok = false

      filters.foreach(f => ok |= fileName.contains(f))

      if (ok) {
        val dir = "/tmp/" + day + "/"
        val localFileName = dir + fileName
        fileDownloader(URL, localFileName)
      }
    })

    return toReturn
  }
}
