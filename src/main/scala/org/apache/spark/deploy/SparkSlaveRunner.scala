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

import java.io.{File, FileOutputStream}
import java.nio.charset.Charset
import java.util
import java.util.zip.ZipFile

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook._
import org.apache.commons.io.{FileUtils, IOUtils}

import scala.collection.JavaConverters._

object SparkSlaveRunner extends Logging {
  def stageZip(request: GetObjectRequest, stagingName: String = "temp.zip", localRoot: File = new File(".").getAbsoluteFile): Boolean = {
    val zipfile = new File(localRoot, stagingName)
    s3.getObject(request, zipfile)
    val zip = new ZipFile(zipfile)
    val entries = zip.entries()
    for (entry <- scala.Stream.continually(if (entries.hasMoreElements) Option(entries.nextElement()) else None).takeWhile(_.isDefined).map(_.get)) {
      if (!entry.isDirectory) {
        val file = new File(localRoot, entry.getName)
        file.getParentFile.mkdirs()
        logger.info("Extracting file: " + file.getAbsolutePath)
        val out = new FileOutputStream(file)
        IOUtils.copy(zip.getInputStream(entry), out)
        out.close()
      }
    }
    zipfile.delete()
  }

  lazy val s3 = {
    AmazonS3ClientBuilder.standard.withRegion(Regions.US_WEST_2).build
  }

  def stage(bucket: String, key: String) = {
    //logger.info(s"Staging $bucket/$key")
    stageZip(new GetObjectRequest(bucket, key))
  }

}

class SparkSlaveRunner(val master: String, val nodeSettings: EC2NodeSettings, override val runner: EC2RunnerLike = EC2Runner) extends EC2Runner with Logging {
  def workerPort: Int = 7076

  def uiPort: Int = 8081

  def memory: String = "4g"

  def properties: Map[String, String] = Map.empty

  override def main(args: Array[String]): Unit = {
    val (node, _) = runner.start(
      nodeSettings = nodeSettings,
      command = node => this,
      javaopts = JAVA_OPTS,
      workerEnvironment = node => new util.HashMap[String, String](Map(
        "SPARK_HOME" -> ".",
        "SPARK_LOCAL_IP" -> node.getStatus.getPrivateIpAddress,
        "SPARK_PUBLIC_DNS" -> node.getStatus.getPublicDnsName,
        "SPARK_WORKER_MEMORY" -> memory
      ).asJava)
    )
    EC2Runner.browse(node, 1080)
    EC2Runner.join(node)
  }

  override def run(): Unit = {
    try {
      //EC2SparkSlaveRunner.stage("simiacryptus", "spark-2.3.1.zip")
      val scalaAssemblyJars = new File("assembly/target/scala-2.11/jars")
      scalaAssemblyJars.mkdirs()
      scalaAssemblyJars.listFiles().foreach(file => {
        //logger.info(s"Deleting $file")
        file.delete()
      })
      val scalaLauncherJars = new File("launcher/target/scala-2.11")
      scalaLauncherJars.mkdirs()
      scalaLauncherJars.listFiles().foreach(file => {
        //logger.info(s"Deleting $file")
        file.delete()
      })

      val localClasspath = System.getProperty("java.class.path")
      logger.info("Java Local Classpath: " + localClasspath)
      localClasspath.split(File.pathSeparator).filter(s => true).map(x => new File(x)).filter(_.exists()).foreach(file => {
        val dest = new File("assembly/target/scala-2.11/jars", file.getName)
        logger.info(s"Copy $file to $dest")
        FileUtils.copyFile(file, dest)
      })
      val lib = new File("lib")
      if (lib.exists()) lib.listFiles().foreach(file => {
        val dest = new File("assembly/target/scala-2.11/jars", file.getName)
        //logger.info(s"Copy $file to $dest")
        FileUtils.copyFile(file, dest)
      })
      System.setProperty("spark.executor.extraClassPath", new File(".").getAbsolutePath + "/lib/*.jar")
      System.setProperty("spark.executor.memory", memory)
      if (null != properties) properties.filter(_._1 != null).filter(_._2 != null).foreach(e => System.setProperty(e._1, e._2))
      FileUtils.write(new File("conf/spark-defaults.conf"), properties.map(e => "%s\t%s".format(e._1, e._2)).mkString("\n"), Charset.forName("UTF-8"))
      org.apache.spark.deploy.worker.Worker.main(Array(
        "--webui-port", uiPort.toString,
        "--port", workerPort.toString,
        "--memory", memory,
        master
      ))
      logger.info(s"Slave init to $master running on ${new File(".").getAbsolutePath}")
      SparkMasterRunner.joinAll()
    } catch {
      case e: Throwable => logger.error("Error running spark master", e)
    } finally {
      EC2Runner.logger.info("Exiting spark master")
      System.exit(0)
    }
  }
}
