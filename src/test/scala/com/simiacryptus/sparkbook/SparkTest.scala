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

import java.util.UUID

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.io.{NotebookOutput, ScalaJson}
import com.simiacryptus.util.lang.SerializableConsumer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object EC2SparkTest extends SparkTest with EC2SparkRunner with AWSNotebookRunner {
  //  nodeSettings = EC2NodeSettings.StandardJavaAMI,
  //  klass = classOf[SparkTest]
  override def numberOfWorkerNodes: Int = 2

  override def driverMemory: String = "16g"

  override def workerMemory: String = "8g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.StandardJavaAMI

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.StandardJavaAMI

}

object EmbeddedSparkTest extends SparkTest with EmbeddedSparkRunner with NotebookRunner {
  //  nodeSettings = EC2NodeSettings.StandardJavaAMI,
  //  klass = classOf[SparkTest]
  override def numberOfWorkerNodes: Int = 2

  override def driverMemory: String = "16g"

  override def workerMemory: String = "8g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.StandardJavaAMI

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.StandardJavaAMI

}

object LocalSparkTest extends SparkTest with LocalRunner with NotebookRunner

class SparkTest extends SerializableConsumer[NotebookOutput]() {
  override def accept(log: NotebookOutput): Unit = {
    log.eval(() => {
      ScalaJson.toJson(System.getProperties.asScala.toArray.toMap)
    })
    val context = SparkContext.getOrCreate()
    log.eval(()=>{
      context.getConf.toDebugString
    })
    log.eval(()=>{
      ScalaJson.toJson(context.getExecutorMemoryStatus)
    })
    var n = 100000
    while(n < 1000000000) {
      log.eval(()=>{
        n.toString
      })
      val uuids : RDD[UUID] = log.eval(()=>{
        context.range(0,n,1).map(x=>UUID.randomUUID()).cache()
      })
      log.eval(()=>{
        ScalaJson.toJson(uuids.map(_.toString).sortBy(_.toString,true).take(100))
      })
      n = n * 10
    }
  }
}
