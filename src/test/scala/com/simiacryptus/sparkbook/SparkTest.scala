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

import java.io.File
import java.util.UUID

import com.simiacryptus.aws.S3Util
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.{NotebookOutput, ScalaJson}
import com.simiacryptus.util.lang.SerializableConsumer
import org.apache.spark.sql.SparkSession

object LocalSparkTest extends SparkTest with LocalRunner with NotebookRunner

object EmbeddedSparkTest extends SparkTest with EmbeddedSparkRunner with NotebookRunner {

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "2g"

}

object EC2SparkTest extends SparkTest with EC2SparkRunner with AWSNotebookRunner {

  override def numberOfWorkerNodes: Int = 1

  override def numberOfWorkersPerNode: Int = 2

  override def driverMemory: String = "2g"

  override def workerMemory: String = "2g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_L

}


abstract class SparkTest extends SerializableConsumer[NotebookOutput]() with Logging {

  override def accept(log: NotebookOutput): Unit = {
    for (i <- 0 until 10) {
      WorkerRunner.distribute(log, (childLog: NotebookOutput, i: Long) => {
        childLog.eval(() => {
          println(s"Hello World (from partition $i)")
          ScalaJson.toJson(LocalAppSettings.read())
        })
      })
      Thread.sleep(30000)
    }
  }
}
