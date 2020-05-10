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
import java.util.Random

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{AwsTendrilNodeSettings, EC2Util, Tendril, TendrilControl}
import com.simiacryptus.ref.wrappers.RefHashMap
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging

/**
  * The type Ec 2 runner.
  */
class DefaultEC2Runner extends EC2RunnerLike with Logging {

  override def start
  (
    nodeSettings: EC2NodeSettings,
    javaOpts: String = "",
    workerEnvironment: EC2Util.EC2Node => java.util.Map[String, String]
  ): (EC2Util.EC2Node, TendrilControl) = {
    val tendrilNodeSettings: AwsTendrilNodeSettings = new AwsTendrilNodeSettings(EC2Runner.envSettings)
    tendrilNodeSettings.instanceType = nodeSettings.machineType
    tendrilNodeSettings.imageId = nodeSettings.imageId
    tendrilNodeSettings.username = nodeSettings.username
    val jvmConfig = tendrilNodeSettings.newJvmConfig()
    jvmConfig.javaOpts += javaOpts
    val localControlPort = new Random().nextInt(1024) + 1024
    val node: EC2Util.EC2Node = tendrilNodeSettings.startNode(EC2Runner.ec2, localControlPort)
    val control = Tendril.startRemoteJvm(node, jvmConfig, localControlPort, Tendril.defaultClasspathFilter _, EC2Runner.s3, workerEnvironment(node), tendrilNodeSettings.bucket)
    List(
      "ec2-settings." + EC2Util.REGION.toString + ".json",
      "user-settings.json"
    ).foreach(configFile => node.scp(new File(configFile), configFile))
    node -> control
  }

}
