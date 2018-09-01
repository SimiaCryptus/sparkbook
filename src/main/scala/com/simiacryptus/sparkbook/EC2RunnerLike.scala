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

import java.util

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, Tendril}
import com.simiacryptus.util.lang.SerializableRunnable

trait EC2RunnerLike {
  def start
  (
    nodeSettings: EC2NodeSettings,
    command: EC2Util.EC2Node => SerializableRunnable,
    javaopts: String = "",
    workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String] = _ => new util.HashMap[String, String]()
  ): (EC2Util.EC2Node, Tendril.TendrilControl)
}

