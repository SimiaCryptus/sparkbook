package com.simiacryptus.sparkbook.repl

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.{AWSNotebookRunner, EC2SparkRunner}

object EC2SparkRepl extends SparkRepl with EC2SparkRunner with AWSNotebookRunner {

  override def s3bucket: String = super.s3bucket

  override def numberOfWorkerNodes: Int = 2

  override def numberOfWorkersPerNode: Int = 2

  override def driverMemory: String = "8g"

  override def workerMemory: String = "8g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_XL

}
