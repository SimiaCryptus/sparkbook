package com.simiacryptus.sparkbook

import java.io.File
import java.util.Random

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{AwsTendrilNodeSettings, EC2Util, Tendril, TendrilControl}
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
    workerEnvironment: EC2Util.EC2Node => java.util.HashMap[String, String]
  ): (EC2Util.EC2Node, TendrilControl) = {
    val tendrilNodeSettings: AwsTendrilNodeSettings = new AwsTendrilNodeSettings(EC2Runner.envSettings)
    tendrilNodeSettings.instanceType = nodeSettings.machineType
    tendrilNodeSettings.imageId = nodeSettings.imageId
    tendrilNodeSettings.username = nodeSettings.username
    val jvmConfig = tendrilNodeSettings.newJvmConfig()
    jvmConfig.javaOpts += javaOpts
    val localControlPort = new Random().nextInt(1024) + 1024
    val node: EC2Util.EC2Node = tendrilNodeSettings.startNode(EC2Runner.ec2, localControlPort)
    val control = Tendril.startRemoteJvm(node, jvmConfig, localControlPort, Tendril.defaultClasspathFilter _, EC2Runner.s3, tendrilNodeSettings.bucket, workerEnvironment(node))
    List(
      "ec2-settings.json",
      "user-settings.json"
    ).foreach(configFile => node.scp(new File(configFile), configFile))
    node -> control
  }

}
