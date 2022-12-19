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
//package com.simiacryptus.sparkbook
//
//import com.simiacryptus.aws.exe.EC2NodeSettings
//
//trait EmbeddedSparkRunner[T <: AnyRef] extends SparkRunner[T] with NotebookRunner[T] {
//
//  final override val numberOfWorkerNodes: Int = 1
//  final override val driverMemory: String = "16g"
//
//  final override def runner: EC2RunnerLike = new LocalBaseRunner
//
//  final override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L
//
//  final override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_L
//
//}
