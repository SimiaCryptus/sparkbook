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

import com.simiacryptus.aws.Tendril
import com.simiacryptus.aws.Tendril.SerializableConsumer
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.util.io.NotebookOutput

class EC2SparkNotebookRunner(val masterSettings: EC2NodeSettings, val workerSettings: EC2NodeSettings, val fns: Array[Tendril.SerializableConsumer[NotebookOutput]]) extends EC2SparkRunner(masterSettings, workerSettings) with NotebookRunner {
  def this(nodeSettings: EC2NodeSettings, klass: Class[_ <: SerializableConsumer[_]]) = this(nodeSettings, nodeSettings, Array(LocalRunner.getTask(klass)))

  def this(masterSettings: EC2NodeSettings, workerSettings: EC2NodeSettings, klass: Class[_ <: SerializableConsumer[_]]) = this(masterSettings, workerSettings, Array(LocalRunner.getTask(klass)))
}
