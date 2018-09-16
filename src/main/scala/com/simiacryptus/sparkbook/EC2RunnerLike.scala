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
import java.util.concurrent.Future

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, Tendril, TendrilControl}
import com.simiacryptus.util.lang.{SerializableRunnable, SerializableSupplier}

trait EC2RunnerLike extends Logging {
  def start
  (
    nodeSettings: EC2NodeSettings,
    javaopts: String = "",
    workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String]
  ): (EC2Util.EC2Node, TendrilControl)


  final def run[T<:AnyRef]
  (
    nodeSettings: EC2NodeSettings,
    command: EC2Util.EC2Node => SerializableSupplier[T],
    javaopts: String = "",
    workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String]
  ): (EC2Util.EC2Node, TendrilControl, Future[T]) = {
    val (node: EC2Util.EC2Node, control: TendrilControl) = start(nodeSettings, javaopts, workerEnvironment)
    try {
      val runnable = command(node)
      logger.info("Updated runnable: " + runnable)
      (node, control, control.start(runnable))
    }
    catch {
      case e: Throwable =>
        node.close()
        throw new RuntimeException(e)
    }
  }

}

