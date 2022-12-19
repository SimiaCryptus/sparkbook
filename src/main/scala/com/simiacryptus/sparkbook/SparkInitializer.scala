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
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
//import org.apache.spark.sql.SparkSession
//
//case class SparkInitializer
//(
//  masterUrl: String,
//  workerMemory: String,
//  numberOfExecutors: Int,
//  hiveRoot: Option[String]
//) {
//  def initSparkEnvironment() = {
//    val builder = SparkSession.builder()
//      .master(masterUrl)
//      .config("fs.s3a.aws.credentials.provider", classOf[DefaultAWSCredentialsProviderChain].getCanonicalName)
//      .config("hive.default.fileformat", "Parquet")
//      .config("spark.io.compression.codec", "lz4")
//    if (hiveRoot.isDefined) {
//      builder.config("spark.sql.warehouse.dir", hiveRoot.get)
//      builder.enableHiveSupport()
//    }
//    builder.config("spark.executor.memory", workerMemory)
//    println("Initializing Spark Context...")
//    try {
//      val sparkSession = builder.getOrCreate()
//      SparkSession.setActiveSession(sparkSession)
//      println(s"Requesting $numberOfExecutors workers")
//      val sparkContext = sparkSession.sparkContext
//      println(s"requestTotalExecutors returned ${sparkContext.requestTotalExecutors(numberOfExecutors, 0, Map.empty)}")
//      val executorMemoryStatus = sparkContext.getExecutorMemoryStatus
//      println(s"Cluster has ${executorMemoryStatus.size} executors: ${executorMemoryStatus.keys.mkString(",")}");
//    } catch {
//      case e: Throwable => e.printStackTrace()
//    }
//    println("Spark Context Initialized")
//  }
//}
