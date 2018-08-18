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

import com.simiacryptus.aws.Tendril
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.util.io.{NotebookOutput, ScalaJson}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object EC2SparkTest extends EC2SparkNotebookRunner(EC2NodeSettings.StandardJavaAMI, 1, classOf[SparkTest]) {
  override def JAVA_OPTS = " -Xmx4g"
}

object LocalSparkTest extends LocalRunner(classOf[SparkTest]) {
}

class SparkTest extends Tendril.SerializableConsumer[NotebookOutput]() {
  override def accept(log: NotebookOutput): Unit = {
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
