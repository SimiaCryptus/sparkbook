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

import com.amazonaws.services.ec2.{AmazonEC2, AmazonEC2ClientBuilder}
import com.amazonaws.services.identitymanagement.{AmazonIdentityManagement, AmazonIdentityManagementClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.simiacryptus.aws.exe.{EmailUtil, UserSettings}
import com.simiacryptus.aws._
import com.simiacryptus.sparkbook.util.{Logging, ScalaJson}
import com.simiacryptus.util.ReportingUtil
import com.simiacryptus.util.test.SysOutInterceptor

import java.io.File
import java.net.URI
import java.util.concurrent.{Executors, TimeUnit}
import scala.language.postfixOps
import scala.util.{Success, Try}

object EC2Runner extends Logging {

  /**
    * Gets ec 2.
    *
    * @return the ec 2
    */
  @transient
  lazy val ec2: AmazonEC2 = AmazonEC2ClientBuilder.standard.withRegion(EC2Util.REGION).build

  /**
    * Gets iam.
    *
    * @return the iam
    */
  @transient
  lazy val iam: AmazonIdentityManagement = AmazonIdentityManagementClientBuilder.standard.withRegion(EC2Util.REGION).build

  /**
    * Gets s 3.
    *
    * @return the s 3
    */
  @transient
  lazy val s3: AmazonS3 = AmazonS3ClientBuilder.standard.withRegion(EC2Util.REGION).build

  lazy val (envSettings, s3bucket, emailAddress) = {
    val envSettings = ScalaJson.cache(new File("ec2-settings." + EC2Util.REGION.toString + ".json"), classOf[AwsTendrilEnvSettings], () => EC2Util.setup(ec2, iam, s3))
    val load = UserSettings.load
    SESUtil.setup(EmailUtil.getSimpleEmailService, load.getEmailAddress)
    (envSettings, envSettings.bucket, load.getEmailAddress)
  }

  def join(node: EC2Util.EC2Node) = {
    var currentCheck: Try[(String, Long)] = Success("running" -> com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis())
    val scheduledExecutorService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build)
    val scheduledTask = scheduledExecutorService.scheduleAtFixedRate(() => {
      currentCheck = Try {
        (node.getStatus.getState.getName, com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis())
      }
    }: Unit, 15, 15, TimeUnit.SECONDS)
    try {
      import scala.concurrent.duration._
      def isRunning = {
        val (status, time) = currentCheck.get
        "running" == status && ((com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis() - time) milliseconds) < (60 seconds)
      }

      while (isRunning) Thread.sleep(30 * 1000)
    } finally {
      scheduledTask.cancel(true)
      scheduledExecutorService.shutdown()
    }
  }

  SysOutInterceptor.INSTANCE.init

  def browse(node: EC2Util.EC2Node, port: Int = 1080): Unit = {
    try
      ReportingUtil.browse(new URI(com.simiacryptus.ref.wrappers.RefString.format("http://%s:" + port + "/", node.getStatus.getPublicDnsName)))
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
    var name = fn.getClass.getSimpleName
    if (null == name || name.isEmpty) name = fn.getClass.toString
    if (null == name || name.isEmpty) name = "index"
    name
  }
}

trait EC2Runner[T <: AnyRef] extends BaseRunner[T] {
  Tendril.getKryo.copy(this) // Test Kryo Copy

//  @transient protected lazy val envTuple: (AwsTendrilEnvSettings, String, ()=>String) = {
//    val envSettings = ScalaJson.cache(
//      new File("ec2-settings." + EC2Util.REGION.toString + ".json"),
//      classOf[AwsTendrilEnvSettings],
//      () => EC2Util.setup(EC2Runner.ec2, EC2Runner.iam, EC2Runner.s3))
//    (envSettings, envSettings.bucket, ()=>{
//      Try {
//        val address = UserSettings.load.getEmailAddress
//        SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient, address)
//        address
//      }.getOrElse("")
//    })
//  }


  def s3bucket: String

  final var emailAddress: String = ""

  //@transient def envSettings: AwsTendrilEnvSettings = envTuple._1

  @transient override def runner: EC2RunnerLike = new DefaultEC2Runner

}


