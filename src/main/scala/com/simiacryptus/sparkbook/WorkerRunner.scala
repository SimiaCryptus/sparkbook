package com.simiacryptus.sparkbook

import java.io.File
import java.net.URI
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import com.simiacryptus.aws.S3Util
import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.util.Random

object WorkerRunner extends Logging {

  def apply[T](parentArchive: URI, fn: (NotebookOutput) => T): T = {
    val spark = SparkSession.builder().getOrCreate()
    val childName = UUID.randomUUID().toString
    val numberOfWorkers = spark.sparkContext.getExecutorMemoryStatus.size
    val r = new AtomicReference[T]()
    WorkerRunner[Object](parentArchive, (x: NotebookOutput) => {
      r.set(fn(x))
      null
    }, childName).get()
    r.get()
  }

  def distribute(fn: (NotebookOutput, Long) => Unit)(implicit log: NotebookOutput, spark: SparkSession = SparkSession.builder().getOrCreate()) = {
    val rdd: RDD[Long] = getClusterRDD(spark)
    map(rdd, fn)
  }

  def map(rdd: RDD[Long], fn: (NotebookOutput, Long) => Unit)(implicit log: NotebookOutput, spark: SparkSession = SparkSession.builder().getOrCreate()) = {
    val parentArchive = log.getArchiveHome
    val results: List[String] = rdd.map(i => {
      val childName = UUID.randomUUID().toString
      try {
        WorkerRunner(parentArchive, (x: NotebookOutput) => {
          fn(x, i)
          null
        }, childName).get()
      } catch {
        case e: Throwable => logger.warn("Error in worker", e)
      }
      childName
    }).collect().toList
    for (id <- results) {
      val root = log.getRoot
      log.p("Subreport: %s %s %s %s", id,
        log.link(new File(root, id + ".md"), "markdown"),
        log.link(new File(root, id + ".html"), "html"),
        log.link(new File(root, id + ".pdf"), "pdf"))
    }
  }

  private def getClusterRDD(spark: SparkSession) = {
    val numberOfWorkers = spark.sparkContext.getExecutorMemoryStatus.size
    val rdd = spark.sparkContext.range(0, numberOfWorkers).repartition(numberOfWorkers)
    rdd
  }

  def mapPartitions[T, U](rdd: RDD[T], fn: (NotebookOutput, Iterator[T]) => Iterator[U])(implicit log: NotebookOutput, cu: ClassTag[U], spark: SparkSession = SparkSession.builder().getOrCreate()) = {
    val parentArchive = log.getArchiveHome
    val results = rdd.mapPartitions(i => {
      val childName = UUID.randomUUID().toString
      try {
        WorkerRunner[Iterator[U]](parentArchive, (x: NotebookOutput) => {
          fn(x, i)
        }, childName).get().map(x => x -> childName)
      } catch {
        case e: Throwable =>
          logger.warn("Error in worker", e)
          throw e
      }
    })
    for (id <- results.map(_._2).distinct().collect()) {
      val root = log.getRoot
      log.p("Subreport: %s %s %s %s", id,
        log.link(new File(root, id + ".md"), "markdown"),
        log.link(new File(root, id + ".html"), "html"),
        log.link(new File(root, id + ".pdf"), "pdf"))
    }
    results.map(_._1)
  }
}

case class WorkerRunner[T](parent: URI, fn: SerializableFunction[NotebookOutput, T], childName: String) extends AWSNotebookRunner[T] {

  override val http_port = 1081 + Random.nextInt(64)

  override def shutdownOnQuit: Boolean = false

  override def s3bucket: String = if (parent.getScheme.startsWith("s3")) parent.getHost else null

  override def emailAddress: String = null

  override def autobrowse = false

  override def apply(log: NotebookOutput): T = {
    logger.info(String.format("Starting report %s at %s", childName, parent))
    log.setAutobrowse(false)
    log.setArchiveHome(parent)
    log.setName(childName)
    val t = fn.apply(log)
    S3Util.upload(log)
    t
  }
}
