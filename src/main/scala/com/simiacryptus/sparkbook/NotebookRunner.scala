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
import java.util.Date
import java.util.regex.Pattern

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.simiacryptus.aws.{S3Util, SESUtil, Tendril}
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.NotebookOutput
import org.apache.commons.io.{FileUtils, IOUtils}

trait NotebookRunner extends WorkerImpl {
  def s3bucket: String
  def emailAddress: String
  def fns: Array[Tendril.SerializableConsumer[NotebookOutput]]

  for (reportTask <- fns) {
    Tendril.getKryo.copy(reportTask)
  }

  def run(): Unit = {
    initWorker()
    try
        for (fn <- this.fns) {
          val testName = EC2Runner.getTestName(fn)
          val startTime = System.currentTimeMillis
          EC2Runner.run((log: NotebookOutput) => {
            log.onComplete((workingDir: File) => {
              EC2Runner.logFiles(workingDir)
              val uploads = S3Util.upload(EC2Runner.s3, s3bucket, "reports/", workingDir)
              sendCompleteEmail(testName, workingDir, uploads, startTime)
            })
            try
              sendStartEmail(testName, fn)
            catch {
              case e@(_: IOException | _: URISyntaxException) =>
                throw new RuntimeException(e)
            }
            fn.accept(log)
            log.setFrontMatterProperty("status", "OK")
          }: Unit, testName)
        }
    finally {
      EC2Runner.logger.info("Exiting node worker")
      System.exit(0)
    }
  }

  private def sendCompleteEmail(testName: String, workingDir: File, uploads: util.Map[File, URL], startTime: Long): Unit = {
    if(null != emailAddress && !emailAddress.isEmpty) {
      var html: String = null
      try
        html = FileUtils.readFileToString(new File(workingDir, testName + ".html"), "UTF-8")
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
        val group = matcher.group(1)
        val imageFile = new File(workingDir, group).getAbsoluteFile
        val url = uploads.get(imageFile)
        if (null == url) {
          EC2Runner.logger.info(String.format("No File Found for %s, reverting to %s", imageFile, group))
          replacedHtml += "\"" + group + "\""
        }
        else {
          EC2Runner.logger.info(String.format("Rewriting %s to %s at %s", group, imageFile, url))
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
      SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient, subject, emailAddress, replacedHtml, replacedHtml, attachments: _*)
    }
  }

  @throws[IOException]
  @throws[URISyntaxException]
  private def sendStartEmail(testName: String, fn: Tendril.SerializableConsumer[NotebookOutput]): Unit = {
    if(null != emailAddress && !emailAddress.isEmpty) {
      val publicHostname = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/public-hostname"), "UTF-8")
      val instanceId = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/instance-id"), "UTF-8")
      val functionJson = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL).enable(SerializationFeature.INDENT_OUTPUT).writer.writeValueAsString(fn)
      //https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=i-00651bdfd6121e199
      val html = "<html><body>" + "<p><a href=\"http://" + publicHostname + ":1080/\">The tiledTexturePaintingPhase can be monitored at " + publicHostname + "</a></p><hr/>" + "<p><a href=\"https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=" + instanceId + "\">View instance " + instanceId + " on AWS Console</a></p><hr/>" + "<p>Script Definition:" + "<pre>" + functionJson + "</pre></p>" + "</body></html>"
      val txtBody = "Process Started at " + new Date
      val subject = testName + " Starting"
      SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient, subject, emailAddress, txtBody, html)
    }
  }

}
