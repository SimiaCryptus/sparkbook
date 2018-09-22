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
