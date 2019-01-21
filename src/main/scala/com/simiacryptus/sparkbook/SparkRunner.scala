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

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.simiacryptus.aws.exe.{EC2NodeSettings, UserSettings}
import com.simiacryptus.aws.{AwsTendrilEnvSettings, AwsTendrilNodeSettings, EC2Util, SESUtil}
import com.simiacryptus.lang.SerializableSupplier
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.{Logging, ScalaJson}
import org.apache.spark.deploy.{SparkMasterRunner, SparkSlaveRunner}

trait SparkRunner[T <: AnyRef] extends SerializableSupplier[T] with Logging {

  @transient protected lazy val envTuple = {
    try {
      val envSettings = ScalaJson.cache(new File("ec2-settings.json"), classOf[AwsTendrilEnvSettings], () => AwsTendrilEnvSettings.setup(EC2Runner.ec2, EC2Runner.iam, EC2Runner.s3))
      SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient, UserSettings.load.emailAddress)
      (envSettings, envSettings.bucket, UserSettings.load.emailAddress)
    } catch {
      case e:Throwable=>
        logger.warn("Err",e)
        (Map.empty,"","")
    }
  }
  @transient protected var masterUrl = "local[4]"

  @transient def emailAddress: String = envTuple._3

  def masterSettings: EC2NodeSettings

  def workerSettings: EC2NodeSettings

  def runner: EC2RunnerLike

  def main(args: Array[String]): Unit = {
    try {
      launch()
    } catch {
      case e: Throwable => logger.warn("Error in application", e)
    }
  }

  def sparkProperties = Map(
    "spark.executor.memory" -> workerMemory,
    "spark.executor.cores" -> workerCores.toString,
    "spark.master" -> masterUrl,
    "spark.app.name" -> getClass.getCanonicalName
  )

  def javaProperties = Map(
    "s3bucket" -> s3bucket
  )

  def launch(): Unit = {
    val masterRunner = new SparkMasterRunner(
      nodeSettings = masterSettings,
      maxHeap = Option(driverMemory),
      sparkProperties = sparkProperties,
      javaProperties = javaProperties
    ) {
      override def runner: EC2RunnerLike = SparkRunner.this.runner
    }
    val (masterNode, masterControl, future) = runner.run(
      masterSettings,
      (node: EC2Util.EC2Node) => {
        logger.info("Setting hostname to " + node.getStatus.getPublicDnsName)
        masterRunner.copy(hostname = node.getStatus.getPublicDnsName)
      },
      workerEnvironment = node => {
        val map = new java.util.HashMap[String, String]()
        map.put("SPARK_WORKER_MEMORY", workerMemory)
        map.put("SPARK_LOCAL_IP", node.getStatus.getPrivateIpAddress)
        map.put("SPARK_PUBLIC_DNS", node.getStatus.getPublicDnsName)
        map
      }
    )
    masterUrl = "spark://" + masterNode.getStatus.getPublicDnsName + ":7077"
    //EC2Runner.browse(masterNode, 8080)
    val slaveRunner = new SparkSlaveRunner(
      master = masterUrl,
      nodeSettings = workerSettings,
      cores = workerCores,
      memory = workerMemory,
      numberOfWorkersPerNode = SparkRunner.this.numberOfWorkersPerNode,
      sparkProperties = sparkProperties,
      javaProperties = javaProperties ++ sparkProperties,
      environment = Map(
        "SPARK_WORKER_MEMORY" -> workerMemory,
        "SPARK_MASTER_HOST" -> masterNode.getStatus.getPublicDnsName,
        "SPARK_WORKER_CORES" -> workerCores.toString
      )
    ) {
      override def runner = SparkRunner.this.runner
    }
    val workers = (1 to numberOfWorkerNodes).par.map(f = i => {
      logger.info(s"Starting worker #$i/$numberOfWorkerNodes")
      runner.run[Object](
        workerSettings,
        (node: EC2Util.EC2Node) => {
          slaveRunner.copy(hostname = node.getStatus.getPublicDnsName, environment = slaveRunner.environment ++ Map(
            "SPARK_LOCAL_IP" -> node.getStatus.getPrivateIpAddress,
            "SPARK_PUBLIC_DNS" -> node.getStatus.getPublicDnsName
          ))
        },
        workerEnvironment = (node: EC2Util.EC2Node) => {
          val map = new java.util.HashMap[String, String]()
          map.put("SPARK_LOCAL_IP", node.getStatus.getPrivateIpAddress)
          map.put("SPARK_PUBLIC_DNS", node.getStatus.getPublicDnsName)
          map
        }
      )
    }).toList
    try {
      val thisInstance: SparkRunner[T] = this
      require(null != masterControl)
      masterControl.start(() => {
        require(null != thisInstance)
        thisInstance.get()
      })
      EC2Runner.browse(masterNode, 1080)
      //EC2Runner.browse(masterNode, 4040)
      EC2Runner.join(masterNode)
      logger.info("Spark task seems to have completed")
    } catch {
      case e : Throwable =>
        logger.warn("Error running task",e)
        throw e
    } finally {
      workers.foreach(_._1.close())
      masterControl.close()
    }
  }

  protected val s3bucket: String = envTuple._2

  def numberOfWorkersPerNode: Int = 1

  def numberOfWorkerNodes: Int = 1

  def driverMemory: String = "7g"

  def workerMemory: String = "6g"
  def workerCores: Int = 1

  private def set(to: AwsTendrilNodeSettings, from: EC2NodeSettings) = {
    to.instanceType = from.machineType
    to.imageId = masterSettings.imageId
    to.username = masterSettings.username
    to
  }

}
