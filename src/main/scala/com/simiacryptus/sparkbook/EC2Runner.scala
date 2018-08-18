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

import java.awt.{Desktop, GraphicsEnvironment}
import java.io.{File, IOException}
import java.net.URI
import java.util
import java.util.Random

import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.{AmazonEC2, AmazonEC2ClientBuilder}
import com.amazonaws.services.identitymanagement.{AmazonIdentityManagement, AmazonIdentityManagementClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.simiacryptus.aws._
import com.simiacryptus.aws.exe.{EC2NodeSettings, UserSettings}
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.Util
import com.simiacryptus.util.io.{MarkdownNotebookOutput, NotebookOutput, ScalaJson}
import com.simiacryptus.util.lang.CodeUtil
import com.simiacryptus.util.test.SysOutInterceptor
import org.slf4j.LoggerFactory

/**
  * The type Ec 2 runner.
  */
object EC2Runner {
  val logger = LoggerFactory.getLogger(classOf[EC2Runner])

  /**
    * Gets ec 2.
    *
    * @return the ec 2
    */
  lazy val ec2: AmazonEC2 = AmazonEC2ClientBuilder.standard.withRegion(Regions.US_EAST_1).build

  /**
    * Gets iam.
    *
    * @return the iam
    */
  lazy val iam: AmazonIdentityManagement = AmazonIdentityManagementClientBuilder.standard.withRegion(Regions.US_EAST_1).build

  /**
    * Gets s 3.
    *
    * @return the s 3
    */
  lazy val s3: AmazonS3 = AmazonS3ClientBuilder.standard.withRegion(Regions.US_WEST_2).build

  /**
    * Gets test name.
    *
    * @param fn the fn
    * @return the test name
    */
  def getTestName(fn: Tendril.SerializableConsumer[NotebookOutput]): String = {
    var name = fn.getClass.getCanonicalName
    if (null == name || name.isEmpty) name = fn.getClass.getSimpleName
    if (null == name || name.isEmpty) name = "index"
    name
  }

  /**
    * Log files.
    *
    * @param f the f
    */
  def logFiles(f: File): Unit = {
    if (f.isDirectory) for (child <- f.listFiles) {
      logFiles(child)
    }
    else logger.info(s"File ${f.getAbsolutePath} length ${f.length}")
  }

  /**
    * Run.
    *
    * @param consumer the consumer
    * @param testName the test name
    */
  def run(consumer: Tendril.SerializableConsumer[NotebookOutput], testName: String): Unit = {
    try {
      val dateStr = Util.dateStr("yyyyMMddHHmmss")
      val log = new MarkdownNotebookOutput(new File("report/" + dateStr + "/" + testName), testName, 1080, true)
      try {
        consumer.accept(log)
        logger.info("Finished worker tiledTexturePaintingPhase")
      } catch {
        case e: Throwable =>
          logger.warn("Error!", e)
      } finally if (log != null) log.close()
    } catch {
      case e: Throwable =>
        logger.warn("Error!", e)
    }
  }

  @throws[IOException]
  def browse(uri: URI): Unit = {
    if (Util.AUTO_BROWSE && !GraphicsEnvironment.isHeadless && Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Desktop.Action.BROWSE)) Desktop.getDesktop.browse(uri)
  }

  SysOutInterceptor.INSTANCE.init

  def join(node: EC2Util.EC2Node) = {
    while ( {
      "running" == node.getStatus.getState.getName
    }) Thread.sleep(30 * 1000)
  }
  def browse(node: EC2Util.EC2Node, port: Int = 1080):Unit = {
    try
        EC2Runner.browse(new URI(String.format("http://%s:" + port + "/", node.getStatus.getPublicDnsName)))
    catch {
      case e: Throwable =>
        EC2Runner.logger.info("Error opening browser", e)
    }
  }


}

import com.simiacryptus.sparkbook.EC2Runner._

abstract class EC2Runner(nodeSettings: EC2NodeSettings) extends Tendril.SerializableRunnable {
  def JAVA_OPTS = " -Xmx50g -Dspark.master=local:4"

  var s3bucket = ""
  var emailFiles = false

  def main(args:Array[String]):Unit = {
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

  def launch(): Unit = {
    val (node,_) = start()
    browse(node, 1080)
    join(node)
  }

  def start(): (EC2Util.EC2Node, Tendril.TendrilControl) = {
    val tendrilNodeSettings: AwsTendrilNodeSettings = new AwsTendrilNodeSettings(envSettings)
    tendrilNodeSettings.instanceType = nodeSettings.machineType
    tendrilNodeSettings.imageId = nodeSettings.imageId
    tendrilNodeSettings.username = nodeSettings.username
    tendrilNodeSettings.jvmConfig.javaOpts += JAVA_OPTS
    tendrilNodeSettings.jvmConfig.javaOpts += " -DGITBASE=\"" + CodeUtil.getGitBase + "\""
    val node = launch(tendrilNodeSettings)
    node
  }

  def command(node: EC2Util.EC2Node): Tendril.SerializableRunnable = this

  def launch(tendrilNodeSettings: AwsTendrilNodeSettings) = {
    val localControlPort = new Random().nextInt(1024) + 1024
    val node: EC2Util.EC2Node = tendrilNodeSettings.startNode(EC2Runner.ec2, localControlPort)
    try {
      val control = Tendril.startRemoteJvm(node, tendrilNodeSettings.jvmConfig, localControlPort, Tendril.defaultClasspathFilter _, EC2Runner.s3, tendrilNodeSettings.bucket, getWorkerEnvironment(node))
      require(null != control)
      control.start(command(node))
      node -> control
    }
    catch {
      case e: Throwable =>
        node.close()
        throw new RuntimeException(e)
    }
  }

  def getWorkerEnvironment(node: EC2Util.EC2Node): util.HashMap[String, String] = {
    new util.HashMap[String, String]()
  }

  def run(): Unit
  var emailAddress = ""
  def isEmailFiles: Boolean = emailFiles

  def setEmailFiles(emailFiles: Boolean): EC2Runner = {
    this.emailFiles = emailFiles
    this
  }
}
