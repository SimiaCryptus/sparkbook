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

import java.io.File
import java.nio.charset.Charset

import org.apache.commons.io.FileUtils

object LocalAppSettings extends Logging {
  def write(config: Map[String, String], workingDir: File = new File(".")): Unit = {
    FileUtils.write(new File(workingDir, "app.json"), ScalaJson.toJson(config), "UTF-8")
  }

  def read(workingDir: File = new File(".").getAbsoluteFile): Map[String, String] = {
    val parentFile = workingDir.getParentFile
    val file = new File(workingDir, "app.json")
    (if (parentFile != null && parentFile.exists()) {
      read(parentFile) ++ Map(
        //file.getAbsolutePath -> "Not Found"
      )
    }
    else Map.empty[String, String]) ++ (
      if (file.exists()) {
        ScalaJson.fromJson(new String(FileUtils.readFileToByteArray(file), Charset.forName("UTF-8")), classOf[Map[String, String]])
      }
      else Map.empty[String, String]
      )
  }

}
