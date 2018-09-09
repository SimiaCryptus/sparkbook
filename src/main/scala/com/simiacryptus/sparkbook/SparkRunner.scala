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

package com.simiacryptus.sparkbook

import java.io.File

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.simiacryptus.aws.exe.{EC2NodeSettings, UserSettings}
import com.simiacryptus.aws.{AwsTendrilEnvSettings, AwsTendrilNodeSettings, EC2Util, SESUtil}
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.ScalaJson
import com.simiacryptus.util.lang.SerializableRunnable
import org.apache.spark.deploy.{SparkMasterRunner, SparkSlaveRunner}

trait SparkRunner extends SerializableRunnable with Logging {
  @transient lazy val (envSettings: AwsTendrilEnvSettings, s3bucket: String, emailAddress: String) = {
    val envSettings = ScalaJson.cache(new File("ec2-settings.json"), classOf[AwsTendrilEnvSettings], () => AwsTendrilEnvSettings.setup(EC2Runner.ec2, EC2Runner.iam, EC2Runner.s3))
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient, UserSettings.load.emailAddress)
    (envSettings, envSettings.bucket, UserSettings.load.emailAddress)
  }

  @transient var masterUrl = "local[4]"

  def masterSettings: EC2NodeSettings

  def workerSettings: EC2NodeSettings

  def numberOfWorkersPerNode: Int = 1

  def runner: EC2RunnerLike

  def main(args: Array[String]): Unit = {
    launch()
  }

  def launch(): Unit = {
    val masterRunner = new SparkMasterRunner(
      nodeSettings = masterSettings,
      runner = runner,
      maxHeap = Option(driverMemory),
      properties = Map(
        "s3bucket" -> envSettings.bucket,
        "spark.executor.memory" -> workerMemory,
        "spark.app.name" -> getClass.getCanonicalName
      ))
    val (masterNode, masterControl) = runner.start(
      masterSettings,
      (node: EC2Util.EC2Node) => {
        logger.info("Setting hostname to " + node.getStatus.getPublicDnsName)
        masterRunner.copy(hostname = node.getStatus.getPublicDnsName)
      },
      javaopts = masterRunner.javaOpts
    )
    masterUrl = "spark://" + masterNode.getStatus.getPublicDnsName + ":7077"
    EC2Runner.browse(masterNode, 8080)
    val workers = (1 to numberOfWorkerNodes).par.map(f = i => {
      logger.info(s"Starting worker #$i/$numberOfWorkerNodes")
      val slaveRunner = SparkSlaveRunner(
        master = masterUrl,
        nodeSettings = workerSettings,
        runner = SparkRunner.this.runner,
        memory = workerMemory,
        numberOfWorkersPerNode = SparkRunner.this.numberOfWorkersPerNode,
        sparkConfig = Map(
          "spark.executor.memory" -> workerMemory,
          "spark.master" -> masterUrl,
          "spark.app.name" -> getClass.getCanonicalName
        ),
        javaConfig = Map(
          "s3bucket" -> envSettings.bucket
        )
      )
      runner.start(
        workerSettings,
        (node: EC2Util.EC2Node) => slaveRunner.copy(hostname = node.getStatus.getPublicDnsName),
        javaopts = slaveRunner.javaOpts
      )
    }).toList
    try {
      val thisInstance = this
      require(null != masterControl)
      masterControl.start(() => {
        require(null != thisInstance)
        thisInstance.run()
      })
      EC2Runner.browse(masterNode, 1080)
      EC2Runner.browse(masterNode, 4040)
      EC2Runner.join(masterNode)
    } finally {
      workers.foreach(_._1.close())
    }
  }

  def numberOfWorkerNodes: Int = 1

  def driverMemory: String = "7g"

  def workerMemory: String = "6g"

  private def set(to: AwsTendrilNodeSettings, from: EC2NodeSettings) = {
    to.instanceType = from.machineType
    to.imageId = masterSettings.imageId
    to.username = masterSettings.username
    to
  }

}
