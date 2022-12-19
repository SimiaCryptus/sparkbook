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
package com.simiacryptus.sparkbook.util

import com.simiacryptus.lang.SerializableSupplier
import com.simiacryptus.util.test.SysOutInterceptor

trait LocalRunner[T] extends SerializableSupplier[T] with Logging {

//  def main(args: Array[String]): Unit = {
//    _main(args)
//  }

  def _main(args: Array[String]): Unit = {
    com.simiacryptus.ref.wrappers.RefSystem.setProperty("spark.master", spark_master)
    com.simiacryptus.ref.wrappers.RefSystem.setProperty("spark.driver.memory", "32g")
    com.simiacryptus.ref.wrappers.RefSystem.setProperty("spark.app.name", "local")
    SysOutInterceptor.INSTANCE.init
    //    SparkSession.setActiveSession(SparkSession.builder()
    //      .config("fs.s3a.aws.credentials.provider", classOf[ProfileCredentialsProvider].getCanonicalName)
    //      .getOrCreate())
    try {
      get()
    } finally {
      logger.warn("Exiting node worker", new RuntimeException("Stack Trace"))
      System.exit(0)
    }
  }

  def spark_master = "local[16]"
}