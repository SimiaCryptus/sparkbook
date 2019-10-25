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
import java.awt.{Graphics2D, RenderingHints}
import java.io.{File, IOException, OutputStream}
import java.util.UUID

import com.simiacryptus.lang.{SerializableFunction, SerializableSupplier}
import com.simiacryptus.notebook.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import com.simiacryptus.util.io.GifSequenceWriter
import javax.imageio.ImageIO
import javax.imageio.stream.MemoryCacheImageOutputStream
import org.apache.commons.io.IOUtils

object NotebookRunner {
  def withMonitoredHtml[T](callback: () => String)(fn: => T)(implicit log: NotebookOutput) = {
    withIFrame(callback, "html", "text/html")(_ => fn)
  }

  def withIFrame[T](callback: () => String, extension: String, mimeType: String)(fn: String => T)(implicit log: NotebookOutput) = {
    val fileName = java.lang.Long.toHexString(MarkdownNotebookOutput.random.nextLong) + "." + extension
    log.p(s"""<iframe src="etc/${fileName}" style="margin: 0px; resize: both; overflow: auto; width:100%; height:400px" ></iframe>""")
    val httpHandle_content = log.getHttpd.addGET("etc/" + fileName, mimeType, (r: OutputStream) => {
      try {
        IOUtils.write(callback(), r, "UTF-8")
      }
      catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    }: Unit)
    try {
      fn("etc/" + fileName)
    } finally {
      try {
        IOUtils.write(callback(), log.file(fileName), "UTF-8")
      } catch {
        case e: Throwable =>
      }
      httpHandle_content.close()
    }
  }

  def withMonitoredJpg[T](contentImage: () => BufferedImage)(fn: => T)(implicit log: NotebookOutput) = {
    val imageName_content = String.format("image_%s.jpg", java.lang.Long.toHexString(MarkdownNotebookOutput.random.nextLong))
    log.p(String.format("<a href=\"etc/%s\"><img src=\"etc/%s\"></a>", imageName_content, imageName_content))

    def writeFile() = {
      try {
        val image = contentImage()
        if (null != image) ImageIO.write(image, "jpeg", log.file(imageName_content))
      } catch {
        case e: Throwable =>
      }
    }

    log.onWrite(new Runnable {
      override def run(): Unit = writeFile()
    })
    val httpHandle_content = log.getHttpd.addGET("etc/" + imageName_content, "image/jpeg", (r: OutputStream) => {
      try {
        val image = contentImage()
        if (null != image) ImageIO.write(image, "jpeg", r)
      }
      catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    }: Unit)
    try {
      fn
    } finally {
      writeFile()
      httpHandle_content.close()
    }
  }


  def gif(images: Seq[BufferedImage], delay: Int = 100)(implicit log: NotebookOutput) = {
    val imageName_content = String.format("image_%s.gif", java.lang.Long.toHexString(MarkdownNotebookOutput.random.nextLong))
    log.p(String.format("<a href=\"etc/%s\"><img src=\"etc/%s\"></a>", imageName_content, imageName_content))
    val fileContent = log.file(imageName_content)
    toGif(fileContent, images, delay, 1200)
    fileContent.close()
  }

  def withMonitoredGif[T](contentImage: () => Seq[BufferedImage], delay: Int = 100)(fn: => T)(implicit log: NotebookOutput) = {
    val imageName_content = String.format("image_%s.gif", java.lang.Long.toHexString(MarkdownNotebookOutput.random.nextLong))
    log.p(String.format("<a href=\"etc/%s\"><img src=\"etc/%s\"></a>", imageName_content, imageName_content))
    val httpHandle_content = log.getHttpd.addGET("etc/" + imageName_content, "image/gif", (outputStream: OutputStream) => {
      try {
        val images = contentImage()
        if (null != images && !images.isEmpty) {
          val fileContent: OutputStream = log.file(imageName_content)
          toGif(fileContent, images, delay, 1200)
          fileContent.close()
          toGif(outputStream, images, delay, 1200)
        }
      }
      catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    }: Unit)
    try {
      fn
    } finally {
      val images = contentImage()
      if (null != images) {
        val fileContent = log.file(imageName_content)
        toGif(fileContent, images, delay, 1200)
        fileContent.close()
      }
      httpHandle_content.close()
    }
  }

  def toGif[T](outputStream: OutputStream, images: Seq[BufferedImage], delay: Int, maxWidth: Int) = {
    if (null != images && 0 < images.length) {
      val imageOutputStream = new MemoryCacheImageOutputStream(outputStream)
      val writer = new GifSequenceWriter(imageOutputStream, images.headOption.map(_.getType).getOrElse(BufferedImage.TYPE_INT_RGB), delay, true)
      val width = images(0).getWidth
      for (image <- images) {
        if (width < maxWidth) {
          writer.writeToSequence(image)
        } else {
          val height = (maxWidth * (image.getHeight.toDouble / image.getWidth.toDouble)).toInt
          writer.writeToSequence(resize(image, maxWidth, height))
        }
      }
      writer.close()
      imageOutputStream.close()
    }
  }

  def resize(source: BufferedImage, width: Int, height: Int): BufferedImage = {
    val image: BufferedImage = new BufferedImage(width, height, source.getType)
    val graphics: Graphics2D = image.getGraphics.asInstanceOf[Graphics2D]
    val hints = new java.util.HashMap[AnyRef, AnyRef]
    hints.put(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC)
    hints.put(RenderingHints.KEY_ALPHA_INTERPOLATION, RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY)
    hints.put(RenderingHints.KEY_COLOR_RENDERING, RenderingHints.VALUE_COLOR_RENDER_QUALITY)
    hints.put(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
    graphics.setRenderingHints(hints)
    graphics.drawImage(source, 0, 0, width, height, null)
    image
  }

}

trait NotebookRunner[T] extends SerializableSupplier[T] with SerializableFunction[NotebookOutput, T] with Logging {
  def get(): T = {
    try {
      val uuid = UUID.randomUUID()
      val log = new MarkdownNotebookOutput(new File(s"report/${name}/${uuid.toString}/"), http_port, false, s"${name}_${uuid.toString}", uuid)
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

  def name: String = getClass.getSimpleName.stripSuffix("$")

  def autobrowse = true

}
