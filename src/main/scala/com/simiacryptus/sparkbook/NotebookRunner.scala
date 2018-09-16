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

import java.io.File

import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.util.Util
import com.simiacryptus.util.io.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.util.lang.{SerializableConsumer, SerializableFunction, SerializableRunnable, SerializableSupplier}

trait NotebookRunner[T] extends SerializableSupplier[T] with SerializableFunction[NotebookOutput,T] with Logging {
  def http_port = 1080

  def autobrowse = true

  def get(): T = {
    try {
      val dateStr = Util.dateStr("yyyyMMddHHmmss")
      val log = new MarkdownNotebookOutput(new File("report/" + dateStr + "/" + name), http_port, autobrowse)
      try {
        val t = apply(log)
        logger.info("Finished worker tiledTexturePaintingPhase")
        t
      } catch {
        case e: Throwable =>
          logger.warn("Error!", e)
          throw e
      } finally if (log != null) log.close()
    } catch {
      case e: Throwable =>
        logger.warn("Error!", e)
        throw e
    }
  }

  def name: String = getClass.getSimpleName

}
