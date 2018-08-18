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
import com.simiacryptus.aws._
import com.simiacryptus.aws.exe.{EC2NodeSettings, UserSettings}
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.ScalaJson

abstract class EC2SparkRunner(masterNodeSettings: EC2NodeSettings, workerNodeSettings: EC2NodeSettings, numberOfWorkers: Int) extends WorkerImpl with Logging {

  def JAVA_OPTS = " -Xmx50g"

  var emailAddress = ""
  var s3bucket = ""
  var emailFiles = false

  def main(args: Array[String]): Unit = {
    launch()
  }

  lazy val envSettings = init()
  def init(): AwsTendrilEnvSettings = {
    val envSettings = ScalaJson.cache(new File("ec2-settings.json"), classOf[AwsTendrilEnvSettings], () => AwsTendrilEnvSettings.setup(EC2Runner.ec2, EC2Runner.iam, EC2Runner.s3))
    s3bucket = envSettings.bucket
    emailAddress = UserSettings.load.emailAddress
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient, emailAddress)
    envSettings
  }

  var masterUrl = "local[4]"

  def launch(): Unit = {
    envSettings.bucket // init
    val properties = Map(
      "s3bucket" -> envSettings.bucket
    )
    val (master, masterControl) = new org.apache.spark.deploy.EC2SparkMasterRunner(masterNodeSettings).start()
    masterUrl = "spark://" + master.getStatus.getPublicDnsName + ":7077"
    EC2Runner.browse(master, 8080)
    val workers = (1 to numberOfWorkers).par.map(i => {
      logger.info(s"Starting worker #$i/$numberOfWorkers")
      val tuple = new org.apache.spark.deploy.EC2SparkSlaveRunner(workerNodeSettings, masterUrl, properties = properties).start()
      tuple
    }).toList
    try {
      masterControl.execute(this)
      EC2Runner.browse(master, 1080)
      EC2Runner.browse(master, 4040)
      EC2Runner.join(master)
    } finally {
      workers.foreach(_._1.close())
    }
  }

  override def initWorker(): Unit = {
    System.setProperty("spark.master",masterUrl)
    System.setProperty("spark.app.name","default")
  }

  private def set(to: AwsTendrilNodeSettings, from: EC2NodeSettings) = {
    to.instanceType = from.machineType
    to.imageId = masterNodeSettings.imageId
    to.username = masterNodeSettings.username
    to
  }

}
