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
import com.simiacryptus.aws.{EC2Util, Tendril}
import com.simiacryptus.sparkbook.{EC2Runner, Logging}
import org.apache.commons.io.FileUtils

import scala.collection.JavaConversions._
import scala.concurrent.duration._
object EC2SparkMasterRunner {
  def joinAll ()= {
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
case class EC2SparkMasterRunner
(
  nodeSettings: EC2NodeSettings,
  controlPort: Int = 7077,
  uiPort: Int = 8080,
  hostname: String = InetAddress.getLocalHost.getHostName,
  memory: String = "4g",
  properties: Map[String, String] = Map.empty
) extends EC2Runner(nodeSettings) with Logging {

  override def JAVA_OPTS: String = s"-Xmx$memory"

  override def command(node: EC2Util.EC2Node): Tendril.SerializableRunnable = {
    this.copy(hostname = node.getStatus.getPublicDnsName)
  }
  override def run(): Unit = {
    try {
      EC2SparkSlaveRunner.stage("simiacryptus", "spark-2.3.1.zip")
      logger.info("Hostname: " + hostname)
      val master = s"spark://$hostname:$controlPort"
      logger.info("Spark master = " + master)
      System.setProperty("spark.master", master)
      System.setProperty("spark.app.name", "default")
      properties.foreach(e => System.setProperty(e._1, e._2))
      FileUtils.write(new File("conf/spark-defaults.conf"), properties.map(e => "%s\t%s".format(e._1, e._2)).mkString("\n"), Charset.forName("UTF-8"))
      org.apache.spark.deploy.master.Master.main(Array(
        "--host", hostname,
        "--port", controlPort.toString,
        "--webui-port", uiPort.toString
      ))
      //SparkContext.setActiveContext(SparkContext.getOrCreate(new SparkConf().setMaster(master).setAppName("default")),false)
      EC2SparkMasterRunner.joinAll()
    } catch {
      case e : Throwable => logger.error("Error running spark master",e)
    } finally {
      EC2Runner.logger.info("Exiting spark master")
      System.exit(0)
    }
  }


  import scala.collection.JavaConverters._
  override def getWorkerEnvironment(node: EC2Util.EC2Node): util.HashMap[String, String] = {
    new util.HashMap[String,String](Map(
      "SPARK_HOME" -> ".",
      "SPARK_LOCAL_IP" -> node.getStatus.getPrivateIpAddress,
      "SPARK_PUBLIC_DNS" -> node.getStatus.getPublicDnsName
    ).asJava)
  }
}
