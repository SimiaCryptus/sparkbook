package com.simiacryptus.sparkbook

import java.net.InetAddress
import java.util

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder
import com.amazonaws.services.ec2.model.{Instance, InstanceState, TerminateInstancesResult}
import com.simiacryptus.aws.EC2Util.EC2Node
import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, Tendril, TendrilControl}
import com.simiacryptus.lang.SerializableCallable
import com.simiacryptus.sparkbook.util.Java8Util._

class LocalBaseRunner extends EC2RunnerLike {

  override def start
  (
    nodeSettings: EC2NodeSettings,
    javaopts: String,
    workerEnvironment: EC2Util.EC2Node => java.util.HashMap[String, String]
  ): (EC2Util.EC2Node, TendrilControl) = {
    val node = new EC2Node(AmazonEC2ClientBuilder.defaultClient(), null, "") {
      /**
        * Gets status.
        *
        * @return the status
        */
      override def getStatus: Instance = {
        new Instance()
          .withPublicDnsName(InetAddress.getLocalHost.getHostName)
          .withState(new InstanceState().withName(status))
      }

      /**
        * Terminate terminate instances result.
        *
        * @return the terminate instances result
        */
      override def terminate(): TerminateInstancesResult = null

      override def close(): Unit = {}
    }
    (node, new TendrilControl(new Tendril.TendrilLink {
      override def isAlive: Boolean = true

      override def exit(): Unit = {}

      override def time(): Long = System.currentTimeMillis()

      override def run[T](task: SerializableCallable[T]): T = task.call()
    }))
  }

  def status = "running"
}
