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
import java.util.concurrent.{Executors, TimeUnit}

import com.amazonaws.regions.Regions
import com.amazonaws.services.ec2.{AmazonEC2, AmazonEC2ClientBuilder}
import com.amazonaws.services.identitymanagement.{AmazonIdentityManagement, AmazonIdentityManagementClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.simiacryptus.aws._
import com.simiacryptus.aws.exe.UserSettings
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.Util
import com.simiacryptus.util.io.ScalaJson
import com.simiacryptus.util.test.SysOutInterceptor

import scala.util.{Success, Try}

object EC2Runner extends Logging {

  /**
    * Gets ec 2.
    *
    * @return the ec 2
    */
  @transient
  lazy val ec2: AmazonEC2 = AmazonEC2ClientBuilder.standard.withRegion(Regions.US_EAST_1).build

  /**
    * Gets iam.
    *
    * @return the iam
    */
  @transient
  lazy val iam: AmazonIdentityManagement = AmazonIdentityManagementClientBuilder.standard.withRegion(Regions.US_EAST_1).build

  /**
    * Gets s 3.
    *
    * @return the s 3
    */
  @transient
  lazy val s3: AmazonS3 = AmazonS3ClientBuilder.standard.withRegion(Regions.US_WEST_2).build

  lazy val (envSettings, s3bucket, emailAddress) = {
    val envSettings = ScalaJson.cache(new File("ec2-settings.json"), classOf[AwsTendrilEnvSettings], () => AwsTendrilEnvSettings.setup(ec2, iam, s3))
    val load = UserSettings.load
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient, load.emailAddress)
    (envSettings, envSettings.bucket, load.emailAddress)
  }

  @throws[IOException]
  def browse(uri: URI): Unit = {
    if (Util.AUTO_BROWSE && !GraphicsEnvironment.isHeadless && Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Desktop.Action.BROWSE)) Desktop.getDesktop.browse(uri)
  }

  SysOutInterceptor.INSTANCE.init

  def join(node: EC2Util.EC2Node) = {
    import Java8Util._
    var currentCheck: Try[(String, Long)] = Success("running" -> System.currentTimeMillis())
    val scheduledExecutorService = Executors.newScheduledThreadPool(1)
    val scheduledTask = scheduledExecutorService.scheduleAtFixedRate(() => {
      currentCheck = Try {
        (node.getStatus.getState.getName, System.currentTimeMillis())
      }
    }: Unit, 15, 15, TimeUnit.SECONDS)
    try {
      import scala.concurrent.duration._
      def isRunning = {
        val (status, time) = currentCheck.get
        "running" == status && ((System.currentTimeMillis() - time) milliseconds) < (60 seconds)
      }

      while (isRunning) Thread.sleep(30 * 1000)
    } finally {
      scheduledTask.cancel(true)
      scheduledExecutorService.shutdown()
    }
  }


  def browse(node: EC2Util.EC2Node, port: Int = 1080): Unit = {
    try
      EC2Runner.browse(new URI(String.format("http://%s:" + port + "/", node.getStatus.getPublicDnsName)))
    catch {
      case e: Throwable =>
        logger.info("Error opening browser", e)
    }
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
    * Gets test name.
    *
    * @param fn the fn
    * @return the test name
    */
  def getTestName(fn: Object): String = {
    var name = fn.getClass.getCanonicalName
    if (null == name || name.isEmpty) name = fn.getClass.getSimpleName
    if (null == name || name.isEmpty) name = "index"
    name
  }

}

trait EC2Runner[T <: AnyRef] extends BaseRunner[T] {
  Tendril.getKryo.copy(this)
  @transient private lazy val envTuple = {
    val envSettings = ScalaJson.cache(new File("ec2-settings.json"), classOf[AwsTendrilEnvSettings], () => AwsTendrilEnvSettings.setup(EC2Runner.ec2, EC2Runner.iam, EC2Runner.s3))
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient, UserSettings.load.emailAddress)
    (envSettings, envSettings.bucket, UserSettings.load.emailAddress)
  }

  @transient def envSettings: AwsTendrilEnvSettings = envTuple._1

  @transient def s3bucket: String = envTuple._2

  @transient def emailAddress: String = envTuple._3

  @transient override def runner: EC2RunnerLike = new DefaultEC2Runner

}


