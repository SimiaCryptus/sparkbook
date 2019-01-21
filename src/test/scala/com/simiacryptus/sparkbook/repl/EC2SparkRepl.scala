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

package com.simiacryptus.sparkbook.repl

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.EC2SparkTest.envTuple
import com.simiacryptus.sparkbook.{AWSNotebookRunner, EC2SparkRunner}

object EC2SparkRepl extends SparkRepl with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {

  override protected val s3bucket: String = envTuple._2

  override def numberOfWorkerNodes: Int = 2

  override def numberOfWorkersPerNode: Int = 2

  override def driverMemory: String = "8g"

  override def workerMemory: String = "8g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_XL

}
