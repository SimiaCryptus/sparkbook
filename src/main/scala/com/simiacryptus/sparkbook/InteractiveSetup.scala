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

import java.util.concurrent.TimeUnit

import com.amazonaws.services.ec2.{AmazonEC2, AmazonEC2ClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.simiacryptus.aws.EC2Util
import com.simiacryptus.notebook.{JsonQuery, MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.ref.wrappers.RefFunction
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.util.CodeUtil

object InteractiveSetup {
  //@JsonIgnore @transient implicit val s3client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(EC2Util.REGION).build()
  //@JsonIgnore @transient implicit val ec2client: AmazonEC2 = AmazonEC2ClientBuilder.standard().withRegion(EC2Util.REGION).build()
}

trait InteractiveSetup[T] extends ScalaReportBase[T] {

  override def apply(log: NotebookOutput): T = {
    log.h1(className)
    log.p(description)
    reference(log)
    val value = new JsonQuery[InteractiveSetup[T]](log.asInstanceOf[MarkdownNotebookOutput]).setMapper({
      new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT)
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        .enable(MapperFeature.USE_STD_BEAN_NAMING)
        .registerModule(DefaultScalaModule)
        .enableDefaultTyping()
    }).setValue(this).print().get(inputTimeoutSeconds, TimeUnit.SECONDS)
    if (monitorRefLog) {
      CodeUtil.withRefLeakMonitor(log, (f: NotebookOutput) => {
        Option(value).getOrElse(InteractiveSetup.this).postConfigure(log)
      })
    } else {
      Option(value).getOrElse(this).postConfigure(log)
    }
  }

  def monitorRefLog = true

  def reference(log: NotebookOutput): Unit = {}

  def className: String = getClass.getSimpleName.stripSuffix("$")

  def description: String = ""

  def inputTimeoutSeconds = 60

  def postConfigure(l: NotebookOutput): T
}
