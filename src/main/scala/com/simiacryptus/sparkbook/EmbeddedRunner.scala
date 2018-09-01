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

import java.net.InetAddress
import java.util

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder
import com.amazonaws.services.ec2.model.{Instance, InstanceState, TerminateInstancesResult}
import com.simiacryptus.aws.EC2Util.EC2Node
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, Tendril}
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.lang.{SerializableCallable, SerializableRunnable}

object EmbeddedRunner extends EC2RunnerLike {

  override def start(nodeSettings: EC2NodeSettings, command: EC2Util.EC2Node => SerializableRunnable, javaopts: String, workerEnvironment: EC2Util.EC2Node => util.HashMap[String, String]): (EC2Util.EC2Node, Tendril.TendrilControl) = {
    val node = new EC2Node(AmazonEC2ClientBuilder.defaultClient(), null, "") {
      /**
        * Gets status.
        *
        * @return the status
        */
      override def getStatus: Instance = new Instance().withPublicDnsName(InetAddress.getLocalHost.getHostName).withState(new InstanceState().withName("running"))

      /**
        * Terminate terminate instances result.
        *
        * @return the terminate instances result
        */
      override def terminate(): TerminateInstancesResult = null

      override def close(): Unit = {}
    }
    val runnable: Runnable = () => command.apply(node).run()
    new Thread(runnable).start()
    node -> new Tendril.TendrilControl(new Tendril.TendrilLink {
      override def isAlive: Boolean = true

      override def exit(): Unit = {}

      override def time(): Long = System.currentTimeMillis()

      override def run[T](task: SerializableCallable[T]): T = task.call()
    })
  }
}

trait EmbeddedRunner extends BaseRunner {
  override def runner: EC2RunnerLike = EmbeddedRunner
}
