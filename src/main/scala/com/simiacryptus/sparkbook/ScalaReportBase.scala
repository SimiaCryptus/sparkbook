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

import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.util.S3Uploader
import org.apache.spark.sql.SparkSession

import scala.concurrent.{ExecutionContext, Future}

trait ScalaReportBase[T] extends SerializableFunction[NotebookOutput, T] {

  def s3bucket: String
  lazy val spark = SparkSession.getActiveSession.get
  require(null != classOf[com.fasterxml.jackson.module.scala.DefaultScalaModule])

  def upload(log: NotebookOutput)(implicit executionContext: ExecutionContext = ExecutionContext.global) = {
    log.write()
    if (!s3bucket.isEmpty) for (archiveHome <- Option(log.getArchiveHome).filter(!_.toString.isEmpty)) {
      val localHome = log.getRoot
      val localName = localHome.getName
      val archiveName = archiveHome.getPath.stripSuffix("/").split('/').last
      lazy val s3 = S3Uploader.buildClientForBucket(archiveHome.getHost)
      if (localHome.isDirectory && localName.equalsIgnoreCase(archiveName)) {
        if (s3bucket != null) {
          S3Uploader.upload(s3, archiveHome.resolve(".."), localHome)
        }
      } else {
        if (s3bucket != null) S3Uploader.upload(s3, archiveHome, localHome)
      }
    }
  }

  def uploadAsync(log: NotebookOutput)(implicit executionContext: ExecutionContext = ExecutionContext.global) = {
    log.write()
    for (archiveHome <- Option(log.getArchiveHome).filter(!_.toString.isEmpty)) {
      Future {
        val localHome = log.getRoot
        val localName = localHome.getName
        val archiveName = archiveHome.getPath.stripSuffix("/").split('/').last
        val s3 = S3Uploader.buildClientForBucket(archiveHome.getHost)
        if (localHome.isDirectory && localName.equalsIgnoreCase(archiveName)) {
          S3Uploader.upload(s3, archiveHome.resolve(".."), localHome)
        } else {
          S3Uploader.upload(s3, archiveHome, localHome)
        }
      }
    }
  }
}
