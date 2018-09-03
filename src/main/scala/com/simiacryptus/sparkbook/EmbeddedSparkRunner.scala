package com.simiacryptus.sparkbook

import com.simiacryptus.aws.exe.EC2NodeSettings

trait EmbeddedSparkRunner extends SparkRunner with NotebookRunner {

  final override def runner: EC2RunnerLike = EmbeddedRunner

  final override def numberOfWorkerNodes: Int = 1

  final override def driverMemory: String = "16g"

  final override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  final override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_L

}
