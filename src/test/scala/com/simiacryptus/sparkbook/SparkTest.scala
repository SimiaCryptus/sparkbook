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

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.sparkbook.repl.SparkSessionProvider
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.{LocalAppSettings, LocalRunner, Logging, ScalaJson}

object LocalSparkTest extends SparkTest with LocalRunner[Object] with NotebookRunner[Object]

object EmbeddedSparkTest extends SparkTest with EmbeddedSparkRunner[Object] with NotebookRunner[Object] {

  override protected val s3bucket: String = envTuple._2
  override val numberOfWorkersPerNode: Int = 2
  override val workerMemory: String = "2g"

  override def hiveRoot: Option[String] = super.hiveRoot

}

object EC2SparkTest extends SparkTest with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {

  override val s3bucket: String = envTuple._2
  override val numberOfWorkerNodes: Int = 1
  override val numberOfWorkersPerNode: Int = 2
  override val driverMemory: String = "2g"
  override val workerMemory: String = "2g"

  override def hiveRoot: Option[String] = super.hiveRoot

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_L

}


abstract class SparkTest extends SerializableFunction[NotebookOutput, Object] with Logging with SparkSessionProvider {

  override def apply(log: NotebookOutput): Object = {
    for (i <- 0 until 3) {
      WorkerRunner.distribute((childLog: NotebookOutput, i: Long) => {
        childLog.eval(() => {
          println(s"Hello World (from partition $i)")
          ScalaJson.toJson(LocalAppSettings.read())
        })
        LocalAppSettings.read().get("worker.index").foreach(idx => {
          com.simiacryptus.ref.wrappers.RefSystem.setProperty("CUDA_DEVICES", idx)
        })
      })(log, spark = spark)
      Thread.sleep(30000)
    }
    null
  }
}
