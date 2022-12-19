///*
// * Copyright (c) 2019 by Andrew Charneski.
// *
// * The author licenses this file to you under the
// * Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance
// * with the License.  You may obtain a copy
// * of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.simiacryptus.sparkbook.repl
//
//import com.simiacryptus.aws.exe.EC2NodeSettings
//import com.simiacryptus.sparkbook.repl.EC2Repl.s3bucket
//import com.simiacryptus.sparkbook.{AWSNotebookRunner, EC2SparkRunner}
//
//import java.net.URI
//import java.util.UUID
//
//object EC2SparkRepl extends SparkRepl with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {
//
//  def s3home: URI = URI.create(s"s3://${s3bucket}/reports/" + UUID.randomUUID().toString + "/")
//
//  override val s3bucket: String = envTuple._2
//  override val numberOfWorkerNodes: Int = 2
//  override val numberOfWorkersPerNode: Int = 2
//  override val driverMemory: String = "8g"
//  override val workerMemory: String = "8g"
//
//  override def className: String = getClass.getName
//
//  override def hiveRoot: Option[String] = super.hiveRoot
//
//  override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L
//
//  override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_XL
//
//}
