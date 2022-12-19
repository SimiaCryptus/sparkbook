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

import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.simiacryptus.notebook.{Jsonable, MarkdownNotebookOutput, NotebookOutput, StringQuery}
import com.simiacryptus.sparkbook.util.Logging
import com.simiacryptus.util.CodeUtil

import java.util.concurrent.TimeUnit

object InteractiveSetup {
  //@JsonIgnore @transient implicit val s3client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(EC2Util.REGION).build()
  //@JsonIgnore @transient implicit val ec2client: AmazonEC2 = AmazonEC2ClientBuilder.standard().withRegion(EC2Util.REGION).build()
}


trait InteractiveSetup[R <: AnyRef, V <: InteractiveSetup[R,V]]
  extends ScalaReportBase[R]
  with Jsonable[V]
  with Logging
{

  override def objectMapper: ObjectMapper = {
    val objectMapper = new ObjectMapper
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT)
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .enable(MapperFeature.USE_STD_BEAN_NAMING)
      .registerModule(DefaultScalaModule)
      .activateDefaultTyping(objectMapper.getPolymorphicTypeValidator)
  }

  override def apply(log: NotebookOutput): R = {
    log.h1(className)
    log.p(description)
    reference(log)
    val query = new StringQuery[V](log.asInstanceOf[MarkdownNotebookOutput]) {
      override protected def toString(value: V): String = {
        Option(value).map(_.toJson()).getOrElse("")
      }

      override protected def fromString(text: String): V = InteractiveSetup.this.fromJson(text)
    }
    query.setValue(this.asInstanceOf[V])
    val value = query.print().get(inputTimeoutSeconds, TimeUnit.SECONDS)
    if (monitorRefLog) {
      CodeUtil.withRefLeakMonitor(log, (f: NotebookOutput) => {
        run(value)(f)
      })
    } else {
      run(value)(log)
    }
  }

  private def run(value: V)(implicit log: NotebookOutput) = {
    try {
      Option(value).getOrElse(InteractiveSetup.this).postConfigure(log)
    } finally {
      logger.info(s"Completed ${value.getClass.getSimpleName}. Write and upload.")
      upload(log)
    }
  }

  def monitorRefLog = true

  def reference(log: NotebookOutput): Unit = {}

  def className: String = getClass.getSimpleName.stripSuffix("$")

  def description: String = ""

  def inputTimeoutSeconds = 60

  def postConfigure(l: NotebookOutput): R
}
