package com.simiacryptus.sparkbook

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

trait AWSNotebookRunner[T] extends BaseAWSNotebookRunner[T] {
  protected def s3bucket: String

  override def s3home: URI = URI.create(s"s3://${s3bucket}/reports/" + new SimpleDateFormat("yyyyMMddmmss").format(new Date()) + "/")
}

object AWSNotebookRunner {
  val runtimeId = UUID.randomUUID().toString
}