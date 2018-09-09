package com.simiacryptus.sparkbook

import java.io.File
import java.net.URI
import java.util.UUID

import com.simiacryptus.aws.S3Util
import com.simiacryptus.util.io.NotebookOutput
import com.simiacryptus.util.lang.SerializableConsumer
import org.apache.spark.sql.SparkSession
import Java8Util._

import scala.util.Random

object WorkerRunner extends Logging {
  def distribute(log: NotebookOutput, fn: (NotebookOutput, Long) => Unit) = {
    val parentArchive = log.getArchiveHome
    val spark = SparkSession.builder().getOrCreate()
    val numberOfWorkers = spark.sparkContext.getExecutorMemoryStatus.size
    val ids = spark.sparkContext.range(0, numberOfWorkers).repartition(numberOfWorkers).map(i => {
      val childName = UUID.randomUUID().toString
      try {
        WorkerRunner(parentArchive, (x: NotebookOutput) => fn(x, i), childName).run()
      } catch {
        case e: Throwable => logger.warn("Error in worker", e)
      }
      childName
    }).collect().toList
    for (id <- ids) {
      val root = log.getRoot
      log.p("Subreport: %s %s %s %s", id,
        log.link(new File(root, id + ".md"), "markdown"),
        log.link(new File(root, id + ".html"), "html"),
        log.link(new File(root, id + ".pdf"), "pdf"))
    }
  }

}

case class WorkerRunner(parent: URI, fn: SerializableConsumer[NotebookOutput], childName: String) extends AWSNotebookRunner {

  override def shutdownOnQuit: Boolean = false

  override def s3bucket: String = if (parent.getScheme.startsWith("s3")) parent.getHost else null

  override def emailAddress: String = null

  override val http_port = 1081 + Random.nextInt(64)

  override def autobrowse = false

  override def accept(log: NotebookOutput): Unit = {
    logger.info(String.format("Starting report %s at %s", childName, parent))
    log.setAutobrowse(false)
    log.setArchiveHome(parent)
    log.setName(childName)
    fn.accept(log)
    S3Util.upload(log)
  }
}
