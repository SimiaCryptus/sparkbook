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

import java.util
import java.util.concurrent.Future

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.aws.{EC2Util, TendrilControl}
import com.simiacryptus.lang.SerializableSupplier
import com.simiacryptus.sparkbook.EC2Runner.{browse, join}

import scala.collection.JavaConverters._

trait BaseRunner[T <: AnyRef] extends SerializableSupplier[T] {
  def nodeSettings: EC2NodeSettings

  def exe(args: String*): T = {
    main(args.toArray)
    this.asInstanceOf
  }

  def main(args: Array[String]): Unit = {
    val (node, _, _) = start()
    browse(node, 1080)
    join(node)
  }

  def start(): (EC2Util.EC2Node, TendrilControl, Future[T]) = {
    runner.run(nodeSettings, _ => this, javaOpts, _ => new java.util.HashMap[String, String](environment.asJava))
  }

  def environment: Map[String, String] = Map("SPARK_HOME" -> ".")

  final def javaOpts: String = List(
    maxHeap.map("-Xmx" + _).toList,
    javaProperties.map(e => "-D" + e._1 + "=" + e._2).toList
  ).flatten.mkString(" ")

  def maxHeap: Option[String] = Option("16g")

  def javaProperties: Map[String, String] = Map(
    "spark.master" -> "local[4]"
  )

  @transient def runner: EC2RunnerLike
}
