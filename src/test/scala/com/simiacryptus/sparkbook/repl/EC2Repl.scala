package com.simiacryptus.sparkbook.repl

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.{AWSNotebookRunner, EC2Runner}

object EC2Repl extends SparkRepl with EC2Runner with AWSNotebookRunner {

  override def nodeSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  override def javaOpts = " -Xmx4g -Dspark.master=local:4"

}
