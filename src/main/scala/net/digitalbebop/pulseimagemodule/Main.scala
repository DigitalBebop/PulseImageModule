package net.digitalbebop.pulseimagemodule


import java.io.File
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.security.SecureRandom
import java.util.UUID
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl._

import com.google.protobuf.ByteString
import net.digitalbebop.ClientRequests.IndexRequest
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.commons.io.FileUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.{HttpClientBuilder, DefaultHttpClient, HttpClients}
import org.apache.http.params.HttpConnectionParams

import scala.collection.JavaConversions._
import scala.util.parsing.json.JSONObject

object Main {
  val startTime = System.currentTimeMillis() / 1000
  final val GALLERY_URL = "https://gallery.csh.rit.edu/v/"
  final val MODULE_NAME = "images"
  final val EXTENSIONS = Array("png", "jpg", "jpeg", "gif", "bmp", "svg")
  val filesProcessed  = new AtomicLong(0)
  
  lazy val getSocketFactory: SSLSocketFactory = {
    val tm: Array[TrustManager] = Array(new NaiveTrustManager())
    val context = SSLContext.getInstance("SSL")
    context.init(Array[KeyManager](), tm, new SecureRandom())
    context.getSocketFactory
  }

  def splitCamelCase(s: String): String = s.replaceAll(String.format("%s|%s|%s",
    "(?<=[A-Z])(?=[A-Z][a-z])", "(?<=[^A-Z])(?=[A-Z])", "(?<=[A-Za-z])(?=[^A-Za-z])"), " ")

  def replaceSplits(s: String) = s.replaceAll("[_|-]", " ")

  def cleanString: String => String = splitCamelCase _ compose replaceSplits

  def validFile(file: File): Boolean = {
    val name = file.getName
    val index = name.lastIndexOf(".")
    if (index != -1) {
      EXTENSIONS.contains(name.substring(index + 1).toLowerCase)
    } else {
      false
    }
  }

  def listFiles(dir: File): Array[File] = if (dir.isDirectory) {
    dir.listFiles().flatMap(listFiles)
  } else {
    val index = dir.getName.lastIndexOf(".")
    if (index != -1 && EXTENSIONS.contains(dir.getName.substring(index + 1).toLowerCase)) {
      Array(dir)
    } else {
      Array.empty
    }
  }

  def getAlbums(dir: File): Set[File] = FileUtils.listFiles(dir, null, true).map(_.getParentFile).toSet

  def processAlbum(dir: File): (String, IndexRequest) = {
    val albums = "albums"
    val strBuilder = new StringBuilder()
    dir.getAbsolutePath.split("/").dropWhile(_ != albums).tail.foreach { dir =>
      strBuilder.append(" " + cleanString(dir))
    }
    val indexData = strBuilder.toString()
    val path = dir.getAbsolutePath
    val moduleId = UUID.nameUUIDFromBytes(dir.getAbsolutePath.getBytes).toString
    val location = GALLERY_URL + path.substring(path.indexOf(albums) + albums.length + 1).replaceAll(" ", "+")
    val title = cleanString(dir.getName) + " - " + cleanString(dir.getParentFile.getName)
    val meta = Map[String, String](
      "format" -> "album",
      "title" -> title,
      "count" -> dir.list.length.toString
    )
    val timestamp = Files.readAttributes(dir.toPath, classOf[BasicFileAttributes]).creationTime().toMillis

    val indexBuilder = IndexRequest.newBuilder()
    indexBuilder.setIndexData(indexData)
    indexBuilder.setLocation(location)
    indexBuilder.setMetaTags(new JSONObject(meta).toString())
    indexBuilder.setModuleId(moduleId)
    indexBuilder.setModuleName(MODULE_NAME)
    indexBuilder.setTimestamp(timestamp)
    indexBuilder.addTags("album")
    (moduleId, indexBuilder.build())
  }

  def processImage(albumId: String, file: File): IndexRequest = {
    val strBuilder = new StringBuilder()
    file.getAbsolutePath.split("/").dropWhile(_ != "albums").tail.foreach(dir =>
      strBuilder.append(" " + dir.replaceAll("[_|-]", " ")))
    val indexData = strBuilder.toString()
    val albums = "albums"

    val path = file.getAbsolutePath
    val moduleId = UUID.nameUUIDFromBytes(file.getAbsolutePath.getBytes).toString
    val timestamp = Files.readAttributes(file.toPath, classOf[BasicFileAttributes]).creationTime().toMillis
    val location = GALLERY_URL + path.substring(path.indexOf(albums) + albums.length + 1).replace(" ", "_")
    val meta = Map(
      "format" -> "image",
      "title" -> cleanString(file.getName)
    )

    val indexBuilder = IndexRequest.newBuilder()
    indexBuilder.setIndexData(indexData)
    indexBuilder.setRawData(ByteString.readFrom(Files.newInputStream(file.toPath)))
    indexBuilder.setLocation(location)
    indexBuilder.setModuleName(MODULE_NAME)
    indexBuilder.setMetaTags(new JSONObject(meta).toString())
    indexBuilder.setTimestamp(timestamp)
    indexBuilder.setModuleId(moduleId)
    indexBuilder.addTags("image")
    indexBuilder.addTags(albumId)
    indexBuilder.build()
  }

  def httpClient() = HttpClientBuilder.create().setDefaultRequestConfig(
    RequestConfig.custom()
      .setConnectTimeout(5000)
      .setConnectionRequestTimeout(5000)
      .setSocketTimeout(5000).build()).build()

  def postMessage(apiServer: String, message: IndexRequest): Unit = {
    val post = new HttpPost(s"$apiServer/api/index")
    post.setEntity(new ByteArrayEntity(message.toByteArray))
    httpClient().execute(post).close()
    //HttpClients.createDefault().execute(post).close()
    val amount = filesProcessed.incrementAndGet()
    if (amount % 100 == 0) {
      val timeDiff = System.currentTimeMillis() / 1000 - startTime
      println(s"processed $amount files, ${(1.0 * amount) / timeDiff} files/sec")
    }
  }

  def main(args: Array[String]): Unit = {

    val options = new Options()
    options.addOption("dir", true, "the directory to recursively look through")
    options.addOption("server", true, "the URL of the API server")

    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)

    val dir = if (cmd.hasOption("dir"))
      cmd.getOptionValue("dir")
    else
      "/"

    val apiServer = if (cmd.hasOption("server"))
      cmd.getOptionValue("server")
    else
      "http://localhost:8080"

    val dirs = getAlbums(new File(dir))
    val queue = new ConcurrentLinkedQueue[File]()
    dirs.foreach(queue.add)

    val cores = Runtime.getRuntime.availableProcessors
    val threads = cores * 2
    val pool = Executors.newFixedThreadPool(threads)
    val lock =  new CountDownLatch(threads)

    (1 to threads).foreach { _ =>
      pool.submit(new Runnable() {
        def run(): Unit = {
          while (true) {
            val file = queue.poll()
            if (file == null) {
              lock.countDown()
              return
            } else {
              val (moduleId, request) = processAlbum(file)
              postMessage(apiServer, request)
              file.listFiles()
                .filter(validFile)
                .map(child => processImage(moduleId, child))
                .foreach(mesg => postMessage(apiServer, mesg))
            }
          }
        }
      })
    }

    lock.await()
    pool.shutdown()
    pool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)

    val endTime = System.currentTimeMillis() / 1000

    println(s"Processed ${filesProcessed.get()} files in ${endTime - startTime} seconds")

  }
}
