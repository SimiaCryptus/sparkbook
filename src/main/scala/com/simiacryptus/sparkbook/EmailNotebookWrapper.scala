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

import com.google.gson.{GsonBuilder, JsonObject, JsonPrimitive}
import com.simiacryptus.aws._
import com.simiacryptus.aws.exe.EmailUtil
import com.simiacryptus.notebook.{Jsonable, NotebookOutput}
import com.simiacryptus.sparkbook.util.Logging
import org.apache.commons.io.IOUtils

import java.io.IOException
import java.net.{InetAddress, URI, URISyntaxException}
import java.util.Date
import scala.util.Try

trait EmailNotebookWrapper[T <: AnyRef with Jsonable[T]] extends Jsonable[T] with Logging {

  var emailAddress: String
  var s3bucket: String

  override def toJson: String = {
    val json = super.toJson
    val gson = new GsonBuilder().setPrettyPrinting().create()
    val jsonObject = gson.fromJson(json, classOf[JsonObject])
    jsonObject.add("emailAddress", new JsonPrimitive(emailAddress))
    jsonObject.add("s3bucket", new JsonPrimitive(s3bucket))
    gson.toJson(jsonObject)
  }

  override def fromJson(text: String): T = {
    val value = super.fromJson(text)
    val jsonObject = new GsonBuilder().create().fromJson(text, classOf[JsonObject])
    if(this.isInstanceOf[EmailNotebookWrapper[_]]) {
      if(jsonObject.has("emailAddress")) value.asInstanceOf[EmailNotebookWrapper[_]].emailAddress = jsonObject.get("emailAddress").getAsString
      if(jsonObject.has("s3bucket")) value.asInstanceOf[EmailNotebookWrapper[_]].s3bucket = jsonObject.get("s3bucket").getAsString
    }
    value
  }

  def withEmailNotifications[R](fn: =>R)(implicit log: NotebookOutput): R = {
    val startTime = System.currentTimeMillis()
    log.onComplete(() => {
      val uploads = S3Util.upload(log);
      if (null != emailAddress && !emailAddress.isEmpty) {
        EmailUtil.sendCompleteEmail(log, startTime, emailAddress, uploads, false)
      }
    })
    try
      sendStartEmail(log.getDisplayName, this)
    catch {
      case e@(_: IOException | _: URISyntaxException) =>
        throw new RuntimeException(e)
    }
    val t = fn
    log.setMetadata("status", "OK")
    t
  }

  def shutdownOnQuit = true

  def autobrowse = true

  def http_port = 1080

  @throws[IOException]
  @throws[URISyntaxException]
  private def sendStartEmail(testName: String, fn: Jsonable[_]): Unit = {
    if (null != emailAddress && !emailAddress.isEmpty) {
      val publicHostname = Try {
        IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/public-hostname"), "UTF-8")
      }.getOrElse(InetAddress.getLocalHost.getHostName)
      val instanceId = Try {
        IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/instance-id"), "UTF-8")
      }.getOrElse("??")
      val functionJson = fn.toJson
      //https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=i-00651bdfd6121e199
      val html = <html>
        <body>
          <h1>
            {testName}
          </h1>
          <p>
            <a href={"http://" + publicHostname + ":1080/"}>The
              {className}
              report can be monitored at
              {publicHostname}
            </a>
          </p>
          <hr/>
          <p>
            <a href={"https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=" + instanceId}>View instance
              {instanceId}
              on AWS Console</a>
          </p>
          <hr/>
          <p>Script Definition:
            <pre>
              {functionJson}
            </pre>
          </p>
        </body>
      </html>
      val txtBody = "Process Started at " + new Date
      val subject = className + " Starting"
      try {
        SESUtil.send(EmailUtil.getSimpleEmailService, subject, emailAddress, txtBody, html.toString())
      } catch {
        case e: Throwable => logger.warn("Error sending email to " + emailAddress, e)
      }
    }
  }

  def className: String

}
