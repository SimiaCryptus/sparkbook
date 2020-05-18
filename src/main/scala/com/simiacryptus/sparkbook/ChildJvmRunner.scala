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

import java.io.File

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder
import com.amazonaws.services.ec2.model.{Instance, InstanceState, TerminateInstancesResult}
import com.simiacryptus.aws.EC2Util.EC2Node
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, Tendril}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging

import scala.collection.JavaConversions._
import scala.util.Random

trait ChildJvmRunner[T <: AnyRef] extends BaseRunner[T] with Logging {
  override lazy val runner: EC2RunnerLike = (_: EC2NodeSettings, subJavaOpts: String, workerEnvironment: EC2Util.EC2Node => java.util.Map[String, String]) => {
    val node = new EC2Node(AmazonEC2ClientBuilder.defaultClient(), null, "") {

      override def getStatus: Instance = {
        new Instance()
          .withPublicDnsName(EC2Util.publicHostname)
          .withState(new InstanceState().withName(""))
      }

      override def terminate(): TerminateInstancesResult = null

      override def close(): Unit = {}
    }
    val env = new java.util.HashMap[String, String](ChildJvmRunner.this.environment)
    env.putAll(workerEnvironment.apply(node))
    val control = Tendril.startLocalJvm(18000 + Random.nextInt(1024), javaOpts + " " + subJavaOpts, env, workingDir)
    (node, control)
  }

  def workingDir = new File(".")

  override def nodeSettings: EC2NodeSettings = null
}