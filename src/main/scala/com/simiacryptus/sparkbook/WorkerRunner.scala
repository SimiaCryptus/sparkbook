/*
 * Copyright (c) 2019 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

  def apply[T](parentArchive: URI, fn: (NotebookOutput) => T, className: String)(implicit spark: SparkSession): T = {
    val childName = UUID.randomUUID().toString
    val numberOfWorkers = spark.sparkContext.getExecutorMemoryStatus.size
    val r = new AtomicReference[T]()
    WorkerRunner[Object](parentArchive, (x: NotebookOutput) => {
      r.set(fn(x))
      null
    }, childName, className).get()
    r.get()
  }

  def distribute(fn: (NotebookOutput, Long) => Unit, className: String)(implicit log: NotebookOutput, spark: SparkSession) = {
    map(rdd, fn, className)
  }

  def map(rdd: RDD[Long], fn: (NotebookOutput, Long) => Unit, className: String)(implicit log: NotebookOutput, spark: SparkSession) = {
    val parentArchive = log.getArchiveHome
    val results: List[String] = repartition(rdd).map(i => {
      val childName = UUID.randomUUID().toString
      try {
        WorkerRunner(parentArchive, (x: NotebookOutput) => {
          fn(x, i)
          null
        }, childName, className).get()
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

  def repartition[T: ClassTag](rdd: RDD[T])(implicit spark: SparkSession): RDD[T] = {
    val numberOfWorkers = spark.sparkContext.getExecutorMemoryStatus.size
    rdd.repartition(numberOfWorkers).cache()
  }

  def distributeEval[T: ClassTag](fn: (Long) => T)(implicit spark: SparkSession) = {
    mapEval(rdd, fn)
  }

  def rdd(implicit spark: SparkSession): RDD[Long] = {
    val numberOfWorkers = spark.sparkContext.getExecutorMemoryStatus.size
    val rdd = spark.sparkContext.range(0, numberOfWorkers).coalesce(numberOfWorkers, true)
    rdd
  }

  def mapEval[T: ClassTag](rdd: RDD[Long], fn: (Long) => T)(implicit spark: SparkSession): List[T] = {
    rdd.map(i => {
      fn(i)
    }).collect().toList
  }

  def mapPartitions[T: ClassTag, U: ClassTag]
  (
    rdd: RDD[T],
    fn: (NotebookOutput, Iterator[T]) => Iterator[U],
    className: String
  )(implicit log: NotebookOutput, spark: SparkSession) = {
    val parentArchive = log.getArchiveHome
    val results = repartition(rdd).mapPartitions(i => {
      try {
        val childName = UUID.randomUUID().toString
        WorkerRunner[Iterator[U]](parentArchive, (x: NotebookOutput) => {
          fn(x, i)
        }, childName, className).get().map(x => x -> childName)
      } catch {
        case e: Throwable =>
          logger.warn("Error in worker", e)
          throw e
      }
    }).cache()
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

case class WorkerRunner[T]
(
  parent: URI,
  fn: SerializableFunction[NotebookOutput, T],
  childName: String,
  className: String
) extends AWSNotebookRunner[T] {

  override val http_port = 1081 + Random.nextInt(64)
  override val s3bucket: String = if (parent.getScheme.startsWith("s3")) parent.getHost else null

  override def shutdownOnQuit: Boolean = false

  override def emailAddress: String = null

  override def autobrowse = false

  override def apply(log: NotebookOutput): T = {
    logger.info(com.simiacryptus.ref.wrappers.RefString.format("Starting report %s at %s", childName, parent))
    log.setArchiveHome(parent)
    log.setDisplayName(childName)
    val t = fn.apply(log)
    S3Util.upload(log)
    t
  }

}
