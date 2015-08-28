package net.digitalbebop.pulseimagemodule


import java.io.{FileInputStream, FilenameFilter, File}
import java.nio.file._
import java.nio.file.attribute.{BasicFileAttributes, FileOwnerAttributeView}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl._

import com.google.protobuf.ByteString
import com.itextpdf.text.pdf.PdfReader
import com.itextpdf.text.pdf.parser.PdfTextExtractor
import com.unboundid.ldap.sdk.{SearchScope, SimpleBindRequest, LDAPConnection}
import net.digitalbebop.ClientRequests.IndexRequest
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{Future, ExecutionContext}
import scala.util.parsing.json.JSONObject



object Main {

  val startTime = System.currentTimeMillis() / 1000
  final val apiServer = "http://localhost:8080"
  final val imageModule = "images"

  val filesProcessed  = new AtomicLong(0)

  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newCachedThreadPool()

    def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    def reportFailure(t: Throwable) {}
  }

  lazy val getSocketFactory: SSLSocketFactory = {
    val tm: Array[TrustManager] = Array(new NaiveTrustManager())
    val context = SSLContext.getInstance("SSL")
    context.init(Array[KeyManager](), tm, new SecureRandom())
    context.getSocketFactory
  }

  def splitCamelCase(s: String): String = s.replaceAll(String.format("%s|%s|%s",
    "(?<=[A-Z])(?=[A-Z][a-z])", "(?<=[^A-Z])(?=[A-Z])", "(?<=[A-Za-z])(?=[^A-Za-z])"), " ")

  def replaceSplits(s: String) = s.replaceAll("[_|-]", " ")

  def cleanString: String => String = splitCamelCase _ compose replaceSplits _

  def getAlbums(dir: File, queue: BlockingQueue[File]): Unit =
    dir.listFiles.filter(_.isDirectory).flatMap(_.listFiles).filter(_.isDirectory).foreach(queue.add)


  def processAlbum(dir: File): IndexRequest = {
    val images = dir.listFiles
    val indexBuilder = IndexRequest.newBuilder()

    val strBuilder = new StringBuilder()
    dir.getAbsolutePath.split("/").dropWhile(_ != "albums").tail.foreach(dir =>
      strBuilder.append(" " + cleanString(dir)))
    val indexData = strBuilder.toString()
    val url = "https://gallery.csh.rit.edu/v"
    val albums = "albums"

    val path = dir.getAbsolutePath
    val location = s"$url/${path.substring(path.indexOf(albums) + albums.length)}"
    val meta = Map(
      "format" -> "image",
      "title" -> cleanString(dir.getName),
      "count" -> images.length
    )
    val timestamp = Files.readAttributes(dir.toPath, classOf[BasicFileAttributes]).creationTime().toMillis

    indexBuilder.setIndexData(indexData)
    indexBuilder.setLocation(location)
    indexBuilder.setMetaTags(new JSONObject(meta).toString())
    indexBuilder.setModuleId(dir.getAbsolutePath)
    indexBuilder.setModuleName(imageModule)
    indexBuilder.setTimestamp(timestamp)
    indexBuilder.build()
  }

  def postMessage(message: IndexRequest): Unit = {
    val post = new HttpPost(s"$apiServer/api/index")
    post.setEntity(new ByteArrayEntity(message.toByteArray))
    HttpClients.createDefault().execute(post).close()
    val amount = filesProcessed.incrementAndGet()
    if (amount % 10 == 0) {
      val timeDiff = System.currentTimeMillis() / 1000 - startTime
      println(s"processed $amount files, ${(1.0 * amount) / timeDiff} files/sec")
    }
  }

  def main(args: Array[String]): Unit = {

    val options = new Options()
    options.addOption("dir", true, "the directory to recursively look through")

    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)

    val dir = if (cmd.hasOption("dir"))
      cmd.getOptionValue("dir")
    else
      "/"

    val queue = new ArrayBlockingQueue[File](1000)
    val POISON_PILL = null

    val f =  new FutureTask[Unit](new Callable[Unit]() {
      def call(): Unit = {
        getAlbums(new File(dir), queue)
        queue.put(POISON_PILL)
      }
    })
    ec.execute(f)

    val workerCount = Runtime.getRuntime.availableProcessors() * 2
    for (i <- 1 to workerCount) {
      ec.execute(new Runnable() {
        def run() : Unit = {
          while (true) {
            val dir = queue.take()
            if (dir == POISON_PILL) {
              queue.put(POISON_PILL)
              return
            } else {
              postMessage(processAlbum(dir))
            }
          }
        }
      })
    }

    ec.threadPool.shutdown()
    ec.threadPool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)

    val endTime = System.currentTimeMillis() / 1000

    println(s"Processed ${filesProcessed.get()} files in ${endTime - startTime} seconds")

  }
}
