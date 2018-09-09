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

import java.io.{File, IOException}
import java.net.{URI, URISyntaxException, URL}
import java.util
import java.util.{Date, UUID}
import java.util.regex.Pattern

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.simiacryptus.aws._
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.Util
import com.simiacryptus.util.io.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.util.lang.{SerializableConsumer, SerializableRunnable}
import org.apache.commons.io.{FileUtils, IOUtils}

object AWSNotebookRunner {
  val runtimeId = UUID.randomUUID().toString
}

trait AWSNotebookRunner extends InnerAWSNotebookRunner {
  def s3bucket: String

  override def s3home: URI = {
    val dateStr = Util.dateStr("yyyyMMddHHmmss")
    URI.create(s"s3://${s3bucket}/reports/$dateStr/")
  }
}

trait InnerAWSNotebookRunner extends SerializableRunnable with SerializableConsumer[NotebookOutput] with Logging {

  def shutdownOnQuit = true
  def emailAddress: String

  def http_port = 1080
  def s3home: URI

  def run(): Unit = {
    try {
      val startTime = System.currentTimeMillis
      val port = http_port
      new NotebookRunner() {
        override def http_port = port
        override def accept(log: NotebookOutput): Unit = {
          log.asInstanceOf[MarkdownNotebookOutput].setArchiveHome(s3home)
          log.onComplete(() => {
            log.write()
            EC2Runner.logFiles(log.getRoot)
            val uploads = S3Util.upload(EC2Runner.s3, log.asInstanceOf[MarkdownNotebookOutput].getArchiveHome, log.getRoot)
            sendCompleteEmail(log.getName.toString, log.getRoot, uploads, startTime)
          })
          try
            sendStartEmail(log.getName.toString, this)
          catch {
            case e@(_: IOException | _: URISyntaxException) =>
              throw new RuntimeException(e)
          }
          InnerAWSNotebookRunner.this.accept(log)
          log.setFrontMatterProperty("status", "OK")
        }

        override def name = EC2Runner.getTestName(InnerAWSNotebookRunner.this)
      }.run()
    }
    finally {
      if (shutdownOnQuit) {
        logger.info("Exiting node worker")
        System.exit(0)
      }
    }
  }


  private def sendCompleteEmail(testName: String, workingDir: File, uploads: util.Map[File, URL], startTime: Long): Unit = {
    if (null != emailAddress && !emailAddress.isEmpty) {
      val reportFile = new File(workingDir, testName + ".html")
      logger.info(String.format("Emailing report at %s to %s", reportFile, emailAddress))
      var html: String = null
      try {
        html = FileUtils.readFileToString(reportFile, "UTF-8")
      }
      catch {
        case e: IOException =>
          html = e.getMessage
      }
      val compile = Pattern.compile("\"([^\"]+?)\"")
      val matcher = compile.matcher(html)
      var start = 0
      var replacedHtml = ""
      while ( {
        matcher.find(start)
      }) {
        replacedHtml += html.substring(start, matcher.start)
        val stringLiteralText = matcher.group(1)
        val imageFile = new File(workingDir, stringLiteralText).getAbsoluteFile
        val url = uploads.get(imageFile)
        if (null == url) {
          logger.info(String.format("No File Found for %s, reverting to %s", imageFile, stringLiteralText))
          replacedHtml += "\"" + stringLiteralText + "\""
        }
        else {
          logger.info(String.format("Rewriting %s to %s at %s", stringLiteralText, imageFile, url))
          replacedHtml += "\"" + url + "\""
        }
        start = matcher.end
      }
      val durationMin = (System.currentTimeMillis - startTime) / (1000.0 * 60)
      val subject = f"$testName Completed in ${durationMin}%.3fmin"
      val zip = new File(workingDir, testName + ".zip")
      val pdf = new File(workingDir, testName + ".pdf")
      val append = "<hr/>" + List(zip, pdf, new File(workingDir, testName + ".html"))
        .map((file: File) => String.format("<p><a href=\"%s\">%s</a></p>", uploads.get(file.getAbsoluteFile), file.getName)).reduce((a: String, b: String) => a + b)
      val endTag = "</body>"
      if (replacedHtml.contains(endTag)) replacedHtml.replace(endTag, append + endTag)
      else replacedHtml += append
      val attachments: Array[File] = Array.empty
      try {
        SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient, subject, emailAddress, replacedHtml, replacedHtml, attachments: _*)
      } catch {
        case e: Throwable => logger.warn("Error sending email to " + emailAddress, e)
      }
    }
  }

  @throws[IOException]
  @throws[URISyntaxException]
  private def sendStartEmail(testName: String, fn: SerializableConsumer[NotebookOutput]): Unit = {
    if (null != emailAddress && !emailAddress.isEmpty) {
      val publicHostname = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/public-hostname"), "UTF-8")
      val instanceId = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/instance-id"), "UTF-8")
      val functionJson = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL).enable(SerializationFeature.INDENT_OUTPUT).writer.writeValueAsString(fn)
      //https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=i-00651bdfd6121e199
      val html = "<html><body>" + "<p><a href=\"http://" + publicHostname + ":1080/\">The tiledTexturePaintingPhase can be monitored at " + publicHostname + "</a></p><hr/>" + "<p><a href=\"https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=" + instanceId + "\">View instance " + instanceId + " on AWS Console</a></p><hr/>" + "<p>Script Definition:" + "<pre>" + functionJson + "</pre></p>" + "</body></html>"
      val txtBody = "Process Started at " + new Date
      val subject = testName + " Starting"
      try {
        SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient, subject, emailAddress, txtBody, html)
      } catch {
        case e: Throwable => logger.warn("Error sending email to " + emailAddress, e)
      }
    }
  }

}
