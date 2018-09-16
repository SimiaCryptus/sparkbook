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
import java.net.InetAddress
import java.nio.charset.Charset
import java.util
import java.util.zip.ZipFile

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{ClasspathUtil, EC2Util}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.util.{LocalAppSettings, Logging}
import org.apache.commons.io.{FileUtils, IOUtils}

import scala.collection.JavaConverters._
import scala.util.Random

object SparkSlaveRunner extends Logging {
  lazy val s3 = {
    AmazonS3ClientBuilder.standard.withRegion(Regions.US_WEST_2).build
  }

  def stage(bucket: String, key: String) = {
    //logger.info(s"Staging $bucket/$key")
    stageZip(new GetObjectRequest(bucket, key))
  }

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

}

case class SparkSlaveRunner
(
  master: String,
  nodeSettings: EC2NodeSettings,
  hostname: String = InetAddress.getLocalHost.getHostName,
  memory: String = "4g",
  numberOfWorkersPerNode: Int = 1,
  sparkConfig: Map[String, String] = Map.empty,
  val javaConfig: Map[String, String] = Map.empty
) extends EC2Runner[Object] with Logging {
  val workerPort: Int = 7078 + Random.nextInt(128)
  val uiPort: Int = 8080 + Random.nextInt(128)

  override def runner: EC2RunnerLike = new DefaultEC2Runner

  override def main(args: Array[String]): Unit = {
    val workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String] = (node: EC2Util.EC2Node) => new util.HashMap[String, String](Map(
      "SPARK_HOME" -> ".",
      "SPARK_LOCAL_IP" -> node.getStatus.getPrivateIpAddress,
      "SPARK_PUBLIC_DNS" -> node.getStatus.getPublicDnsName,
      "SPARK_WORKER_MEMORY" -> memory,
      "SPARK_WORKER_INSTANCES" -> numberOfWorkersPerNode.toString
    ).asJava)
    val (node: EC2Util.EC2Node, _, _) = runner.run[Object](
      nodeSettings = nodeSettings,
      command = (node: EC2Util.EC2Node) => () => {
        this.get()
        null
      }: Object,
      javaopts = javaOpts,
      workerEnvironment = workerEnvironment
    )
    EC2Runner.join(node)
  }

  override def get(): Object = {
    try {
      //if (null != javaConfig) javaConfig.filter(_._1 != null).filter(_._2 != null).foreach(e => System.setProperty(e._1, e._2))
      for (i <- 0 until numberOfWorkersPerNode) {
        val workingDir = new File(s"spark_workers${File.separator}%d".format(i))
        workingDir.mkdirs()
        prepareWorkingDirectory(workingDir, i)
        val conf = new File(workingDir, s"conf${File.separator}spark-defaults.conf")
        FileUtils.write(conf, (sparkConfig ++ Map(
          "spark.executor.extraClassPath" -> (new File(".").getAbsolutePath + "/lib/*.jar"),
          "spark.executor.extraJavaOptions" -> javaConfig.map(e => s"-D${e._1}=${e._2}").mkString(" "),
          "spark.executor.memory" -> memory,
          "spark.shuffle.service.enabled" -> "false"
        )).map(e => "%s\t%s".format(e._1, e._2 //.replaceAll(":", "\\\\:")
        )).mkString("\n"), Charset.forName("UTF-8"))
        SingleSlaveRunner(args = Array(
          "--webui-port", (uiPort + i).toString,
          "--port", (workerPort + i).toString,
          "--memory", memory,
          master
        ), workingDir = workingDir, environment = Map(
          "SPARK_HOME" -> workingDir.getAbsolutePath
        )).start()
      }
      logger.info(s"Slave init to $master running on ${new File(".").getAbsolutePath}")
      SparkMasterRunner.joinAll()
    } catch {
      case e: Throwable => logger.error("Error running spark slave", e)
    } finally {
      logger.info("Exiting spark slave")
      System.exit(0)
    }
    null
  }

  final def copyFile(src: File, dest: File, retries: Int = 2): Unit = {
    try {
      if (!dest.getAbsoluteFile.equals(src.getAbsoluteFile)) FileUtils.copyFile(src, dest)
    } catch {
      case e: Throwable if (retries > 0) =>
        Thread.sleep(100)
        copyFile(src, dest, retries - 1)
    }
  }

  private def prepareWorkingDirectory(workingDir: File, workerNumber: Int) = {
    //EC2SparkSlaveRunner.stage("simiacryptus", "spark-2.3.1.zip")
    val scalaAssemblyJars = new File(workingDir, "assembly/target/scala-2.11/jars")
    scalaAssemblyJars.mkdirs()
    scalaAssemblyJars.listFiles().foreach(file => {
      //logger.info(s"Deleting $file")
      file.delete()
    })
    val scalaLauncherJars = new File(workingDir, "launcher/target/scala-2.11")
    scalaLauncherJars.mkdirs()

    val localClasspath = System.getProperty("java.class.path")
    logger.info("Java Local Classpath: " + localClasspath)
    localClasspath.split(File.pathSeparator).filter(s => !s.contains("idea_rt")
      && !s.contains(File.separator + "jre" + File.separator)
      && !s.contains(File.separator + "jdk" + File.separator)
    ).map(x => new File(x)).filter(_.exists()).foreach(file => {
      var src = file
      if (src.isDirectory) src = ClasspathUtil.toJar(src)
      var dest = new File(workingDir, "assembly/target/scala-2.11/jars/" + src.getName)
      logger.debug(s"Copy $src to $dest")
      try {
        copyFile(src, dest)
      } catch {
        case e: Throwable => logger.info("Error copying file " + src + " to " + dest, e)
      }
    })

    LocalAppSettings.write(Map(
      "worker.index" -> workerNumber.toString
    ), workingDir)
  }


}

case class SingleSlaveRunner
(
  val args: Array[String],
  override val workingDir: File = new File("."),
  override val environment: Map[String, String] = Map()
) extends ChildJvmRunner[Object] {

  override def maxHeap: Option[String] = Option("1g")

  override def get(): Object = {
    javaProperties.filter(_._1 != null).filter(_._2 != null).foreach(e => System.setProperty(e._1, e._2))
    System.setProperty("spark.executor.extraJavaOptions", javaProperties.map(e => s"-D${e._1}=${e._2}").mkString(" "))
    System.getProperties.asScala.foreach(e => System.out.println("Spark Work Init Property: " + e._1 + " = " + e._2))
    fn(args)
    null
  }

  def fn: Array[String] => Unit = org.apache.spark.deploy.worker.Worker.main _
}
