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
import com.simiacryptus.util.io.{NotebookOutput, ScalaJson}
import com.simiacryptus.util.lang.{CodeUtil, SerializableConsumer, SerializableRunnable}
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

  lazy val (envSettings, s3bucket, emailAddress) = {
    val envSettings = ScalaJson.cache(new File("ec2-settings.json"), classOf[AwsTendrilEnvSettings], () => AwsTendrilEnvSettings.setup(EC2Runner.ec2, EC2Runner.iam, EC2Runner.s3))
    val load = UserSettings.load
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient, load.emailAddress)
    (envSettings, envSettings.bucket, load.emailAddress)
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

  /**
    * Gets test name.
    *
    * @param fn the fn
    * @return the test name
    */
  def getTestName(fn: SerializableConsumer[NotebookOutput]): String = {
    var name = fn.getClass.getCanonicalName
    if (null == name || name.isEmpty) name = fn.getClass.getSimpleName
    if (null == name || name.isEmpty) name = "index"
    name
  }

  def launch
  (
    nodeSettings: EC2NodeSettings,
    command: EC2Util.EC2Node => SerializableRunnable,
    javaopts: String = "",
    workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String] = _ => new util.HashMap[String, String]()
  ): Unit = {
    val (node, _) = start(nodeSettings, command, javaopts = javaopts, workerEnvironment)
    browse(node, 1080)
    join(node)
  }

  def browse(node: EC2Util.EC2Node, port: Int = 1080): Unit = {
    try
      EC2Runner.browse(new URI(String.format("http://%s:" + port + "/", node.getStatus.getPublicDnsName)))
    catch {
      case e: Throwable =>
        EC2Runner.logger.info("Error opening browser", e)
    }
  }

  def start
  (
    nodeSettings: EC2NodeSettings,
    command: EC2Util.EC2Node => SerializableRunnable,
    javaopts: String = "",
    workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String] = _ => new util.HashMap[String, String]()
  ): (EC2Util.EC2Node, Tendril.TendrilControl) = {
    val tendrilNodeSettings: AwsTendrilNodeSettings = new AwsTendrilNodeSettings(envSettings)
    tendrilNodeSettings.instanceType = nodeSettings.machineType
    tendrilNodeSettings.imageId = nodeSettings.imageId
    tendrilNodeSettings.username = nodeSettings.username
    tendrilNodeSettings.jvmConfig.javaOpts += javaopts
    tendrilNodeSettings.jvmConfig.javaOpts += " -DGITBASE=\"" + CodeUtil.getGitBase + "\""
    val node = launch(tendrilNodeSettings, command, workerEnvironment)
    node
  }

  def launch
  (
    tendrilNodeSettings: AwsTendrilNodeSettings,
    command: EC2Util.EC2Node => SerializableRunnable,
    workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String]
  ) = {
    val localControlPort = new Random().nextInt(1024) + 1024
    val node: EC2Util.EC2Node = tendrilNodeSettings.startNode(EC2Runner.ec2, localControlPort)
    try {
      val control = Tendril.startRemoteJvm(node, tendrilNodeSettings.jvmConfig, localControlPort, Tendril.defaultClasspathFilter _, EC2Runner.s3, tendrilNodeSettings.bucket, workerEnvironment(node))
      require(null != control)
      List(
        "ec2-settings.json",
        "user-settings.json"
      ).foreach(configFile => node.scp(new File(configFile), configFile))
      control.start(command(node))
      node -> control
    }
    catch {
      case e: Throwable =>
        node.close()
        throw new RuntimeException(e)
    }
  }


}

trait EC2Runner extends SerializableRunnable {
  def nodeSettings: EC2NodeSettings

  def s3bucket: String = EC2Runner.s3bucket

  def emailAddress: String = EC2Runner.emailAddress

  def exe[T](args: String*): T = {
    require(this.isInstanceOf[T])
    main(args.toArray)
    this.asInstanceOf
  }

  def main(args: Array[String]): Unit = {
    EC2Runner.launch(nodeSettings, (node: EC2Util.EC2Node) => EC2Runner.this, javaopts = JAVA_OPTS)
  }

  def start(): (EC2Util.EC2Node, Tendril.TendrilControl) = {
    EC2Runner.start(nodeSettings, (node: EC2Util.EC2Node) => EC2Runner.this, javaopts = JAVA_OPTS)
  }

  def JAVA_OPTS = " -Xmx50g -Dspark.master=local:4"
}
