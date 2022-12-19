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
//import com.simiacryptus.sparkbook.{EmbeddedSparkRunner, NotebookRunner}
//
//object EmbeddedSparkRepl extends SparkRepl with EmbeddedSparkRunner[Object] with NotebookRunner[Object] {
//
//  override val s3bucket: String = envTuple._2
//  override val numberOfWorkersPerNode: Int = 2
//  override val workerMemory: String = "8g"
//
//  override def hiveRoot: Option[String] = super.hiveRoot
//
//}
