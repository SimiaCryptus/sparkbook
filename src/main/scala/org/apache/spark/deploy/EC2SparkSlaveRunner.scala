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
import java.util
import java.util.zip.ZipFile

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.simiacryptus.aws.EC2Util
import com.simiacryptus.sparkbook._
import org.apache.commons.io.{FileUtils, IOUtils}

object EC2SparkSlaveRunner extends Logging {
  def stageZip(request: GetObjectRequest, stagingName: String = "temp.zip", localRoot: File = new File(".").getAbsoluteFile): Boolean = {
    val zipfile = new File(localRoot, stagingName)
    s3.getObject(request, zipfile)
    val zip = new ZipFile(zipfile)
    val entries = zip.entries()
    for (entry <- Stream.continually(if (entries.hasMoreElements) Option(entries.nextElement()) else None).takeWhile(_.isDefined).map(_.get)) {
      if(!entry.isDirectory) {
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
class EC2SparkSlaveRunner
(
  nodeSettings: EC2NodeSettings,
  master: String,
  workerPort: Int = 7076,
  uiPort: Int = 8080,
  memory: String = "4g",
  properties: Map[String,String] = Map.empty
) extends EC2Runner(nodeSettings) with Logging {

  override def run(): Unit = {
    try {
      EC2SparkSlaveRunner.stage("simiacryptus", "spark-2.3.1.zip")
      new File("assembly/target/scala-2.11/jars").listFiles().foreach(file=>{
        logger.info(s"Deleting $file")
        file.delete()
      })
      new File("lib").listFiles().foreach(file=>{
        val dest = new File("assembly/target/scala-2.11/jars", file.getName)
        logger.info(s"Copy $file to $dest")
        FileUtils.copyFile(file, dest)
      })
      System.setProperty("spark.executor.extraClassPath",new File(".").getAbsolutePath+"/lib/*.jar")
      System.setProperty("spark.executor.memory",memory)
      org.apache.spark.deploy.worker.Worker.main(Array(
        "--webui-port", uiPort.toString,
        "--port", workerPort.toString,
        master
      ))
      logger.info(s"Slave init to $master running on ${new File(".").getAbsolutePath}")
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
      "SPARK_PUBLIC_DNS" -> node.getStatus.getPublicDnsName,
      "SPARK_WORKER_MEMORY" -> memory
    ).asJava)
  }
}
