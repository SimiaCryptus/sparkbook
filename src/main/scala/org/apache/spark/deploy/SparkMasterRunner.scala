/*
 * Copyright (c) 2018 by Andrew Charneski.
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

package org.apache.spark.deploy

import java.io.File
import java.net.InetAddress
import java.nio.charset.Charset
import java.util

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.EC2Runner.{browse, join}
import com.simiacryptus.sparkbook.{DefaultEC2Runner, EC2Runner, EC2RunnerLike, Logging}
import com.simiacryptus.util.io.KryoUtil
import org.apache.commons.io.FileUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration._

object SparkMasterRunner {
  def joinAll() = {
    val currentThread = Thread.currentThread()
    val wasDaemon = currentThread.isDaemon
    Thread.sleep((1 minute).toMillis)
    //currentThread.setDaemon(true)
    Stream.continually(Thread.getAllStackTraces.toMap.filter(!_._1.isDaemon).filter(_._1 != currentThread))
      .takeWhile(!_.isEmpty).foreach(runningThreads => {
      for (thread <- runningThreads) {
        println("Running: " + thread._1.getName)
      }
      Thread.sleep((1 minute).toMillis)
    })
    currentThread.setDaemon(wasDaemon)
  }

}

case class SparkMasterRunner
(
  nodeSettings: EC2NodeSettings,
  properties: Map[String, String] = Map.empty,
  hostname: String = InetAddress.getLocalHost.getHostName,
  override val maxHeap: Option[String] = Option("4g")
) extends EC2Runner[Object] with Logging {
  override def runner: EC2RunnerLike = new DefaultEC2Runner

  override def get(): Object = {
    try {
      //EC2SparkSlaveRunner.stage("simiacryptus", "spark-2.3.1.zip")
      logger.info("Hostname: " + hostname)
      logger.info("this: " + this)
      val master = s"spark://$hostname:$controlPort"
      logger.info("Spark master = " + master)
      System.setProperty("spark.master", master)
      System.setProperty("spark.app.name", "default")
      if (null != properties) properties.filter(_._1 != null).filter(_._2 != null).foreach(e => System.setProperty(e._1, e._2))
      FileUtils.write(new File("conf/spark-defaults.conf"), properties.map(e => "%s\t%s".format(e._1, e._2)).mkString("\n"), Charset.forName("UTF-8"))
      org.apache.spark.deploy.master.Master.main(Array(
        "--host", hostname,
        "--port", controlPort.toString,
        "--webui-port", uiPort.toString
      ))
      //SparkContext.setActiveContext(SparkContext.getOrCreate(new SparkConf().setMaster(master).setAppName("default")),false)
      SparkMasterRunner.joinAll()
    } catch {
      case e: Throwable => logger.error("Error running spark master", e)
    } finally {
      logger.info("Exiting spark master")
      System.exit(0)
    }
    null
  }

  def controlPort: Int = 7077

  def uiPort: Int = 8080

}
