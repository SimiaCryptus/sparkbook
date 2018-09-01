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

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, Tendril}
import com.simiacryptus.sparkbook.EC2Runner.{browse, join}
import com.simiacryptus.util.lang.SerializableRunnable

trait BaseRunner extends SerializableRunnable {
  def nodeSettings: EC2NodeSettings

  def exe[T](args: String*): T = {
    require(this.isInstanceOf[T])
    main(args.toArray)
    this.asInstanceOf
  }

  def main(args: Array[String]): Unit = {
    val (node, _) = runner.start(nodeSettings, (node: EC2Util.EC2Node) => BaseRunner.this, javaopts = JAVA_OPTS)
    browse(node, 1080)
    join(node)
  }

  def runner: EC2RunnerLike

  def start(): (EC2Util.EC2Node, Tendril.TendrilControl) = {
    runner.start(nodeSettings, (node: EC2Util.EC2Node) => BaseRunner.this, javaopts = JAVA_OPTS)
  }

  def JAVA_OPTS = " -Xmx50g -Dspark.master=local:4"
}
