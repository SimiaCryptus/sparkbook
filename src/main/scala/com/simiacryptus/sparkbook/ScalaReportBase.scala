/*
 * Copyright (c) 2020 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.sparkbook

import java.net.URI

import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.util.S3Uploader

import scala.concurrent.{ExecutionContext, Future}

trait ScalaReportBase[R] extends SerializableFunction[NotebookOutput, R] //with SparkSessionProvider
{

  def s3bucket: String

  require(null != classOf[com.fasterxml.jackson.module.scala.DefaultScalaModule])

  def upload(log: NotebookOutput) = {
    log.write()
    val dest = getArchiveHome(log)
    for (archiveHome <- dest) {
      lazy val s3 = S3Uploader.buildClientForBucket(archiveHome.getHost)
      S3Uploader.upload(s3, archiveHome, log.getRoot)
    }
  }

  def upload_noID(log: NotebookOutput, overwrite: Boolean) = {
    log.write()
    val dest = getArchiveHome(log)
    for (archiveHome <- dest) {
      lazy val s3 = S3Uploader.buildClientForBucket(archiveHome.getHost)
      if (overwrite) S3Uploader.rmDir(s3, archiveHome)
      S3Uploader.uploadDir(s3, archiveHome, log.getRoot)
    }
  }

  def uploadAsync(log: NotebookOutput)(implicit executionContext: ExecutionContext = ExecutionContext.global) = {
    log.write()
    val dest = getArchiveHome(log)
    for (archiveHome <- dest) {
      Future {
        val localHome = log.getRoot
        val s3 = S3Uploader.buildClientForBucket(archiveHome.getHost)
        S3Uploader.upload(s3, archiveHome, localHome)
      }
    }
  }

  def getArchiveHome(log: NotebookOutput) = {
    val archiveHome = Option(log.getArchiveHome)
      .filter(!_.toString.isEmpty)
      .filter(_.getHost != null)
      .filter(!_.getHost.isEmpty)
      .filter(_.getHost != "null")
    if (archiveHome.isDefined) {
      val logRoot = log.getRoot
      val archiveName = archiveHome.get.getPath.stripSuffix("/").split('/').last
      if (logRoot.isDirectory && logRoot.getName.equalsIgnoreCase(archiveName)) {
        Option(archiveHome.get.resolve(".."))
      } else {
        Option(archiveHome.get)
      }
    } else {
      if (s3bucket != null && !s3bucket.isEmpty) {
        Option(new URI(s"s3://$s3bucket/${log.getFileName}/${log.getId}/"))
      } else {
        None
      }
    }
  }
}
