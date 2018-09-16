/*
 * Copyright (c) 2018 by Andrew Charneski.
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
package com.simiacryptus.sparkbook

import com.simiacryptus.util.lang.{SerializableRunnable, SerializableSupplier}
import com.simiacryptus.util.test.SysOutInterceptor

trait LocalRunner[T] extends SerializableSupplier[T] {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.master", "local[16]")
    System.setProperty("spark.app.name", "local")
    SysOutInterceptor.INSTANCE.init
    try {
      get()
    } finally {
      System.exit(0)
    }

  }
}