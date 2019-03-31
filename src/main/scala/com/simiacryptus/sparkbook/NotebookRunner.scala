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

package com.simiacryptus.sparkbook

import java.awt.image.BufferedImage
import java.io.{File, IOException, OutputStream}

import com.simiacryptus.lang.{SerializableFunction, SerializableSupplier}
import com.simiacryptus.notebook.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import com.simiacryptus.util.Util
import javax.imageio.ImageIO

object NotebookRunner {
  def withMonitoredImage[T](log: NotebookOutput, contentImage: => BufferedImage)(fn: => T) = {
    val imageName_content = String.format("etc/image_%s.jpg", java.lang.Long.toHexString(MarkdownNotebookOutput.random.nextLong))
    log.p(String.format("<a href=\"%s\"><img src=\"%s\"></a>", imageName_content, imageName_content))
    val httpHandle_content = log.getHttpd.addGET(imageName_content, imageName_content + "/jpeg", (r: OutputStream) => {
      try
        ImageIO.write(contentImage, "jpeg", r)
      catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    } : Unit)
    try {
      fn
    } finally {
      try {
        httpHandle_content.close()
        ImageIO.write(contentImage, "jpeg", log.file(imageName_content))
      } catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    }
  }

}

trait NotebookRunner[T] extends SerializableSupplier[T] with SerializableFunction[NotebookOutput, T] with Logging {
  def get(): T = {
    try {
      val dateStr = Util.dateStr("yyyyMMddHHmmss")
      val log = new MarkdownNotebookOutput(new File("report/" + dateStr + "/" + name), http_port, false)
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

  def http_port = 1080

  def autobrowse = true

  def name: String = getClass.getSimpleName

}
