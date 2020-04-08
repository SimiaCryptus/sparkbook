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

import java.util.concurrent.Future

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, TendrilControl}
import com.simiacryptus.lang.SerializableSupplier
import com.simiacryptus.ref.wrappers.RefHashMap
import com.simiacryptus.sparkbook.util.Logging

trait EC2RunnerLike extends Logging {
  def start
  (
    nodeSettings: EC2NodeSettings,
    javaOpts: String = "",
    workerEnvironment: EC2Util.EC2Node => RefHashMap[String, String]
  ): (EC2Util.EC2Node, TendrilControl)


  final def run[T <: AnyRef]
  (
    nodeSettings: EC2NodeSettings,
    command: EC2Util.EC2Node => SerializableSupplier[T],
    javaOpts: String = "",
    workerEnvironment: EC2Util.EC2Node => RefHashMap[String, String]
  ): (EC2Util.EC2Node, TendrilControl, Future[T]) = {
    val (node: EC2Util.EC2Node, control: TendrilControl) = start(nodeSettings, javaOpts, workerEnvironment)
    try {
      val runnable = command(node)
      logger.info("Updated runnable: " + runnable)
      (node, control, control.start(() => runnable.get()))
    }
    catch {
      case e: Throwable =>
        node.close()
        throw new RuntimeException(e)
    }
  }

}

