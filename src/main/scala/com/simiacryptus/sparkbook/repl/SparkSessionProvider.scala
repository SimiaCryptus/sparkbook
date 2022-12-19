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

package com.simiacryptus.sparkbook.repl

//import com.esotericsoftware.kryo.Kryo
//import com.simiacryptus.sparkbook.util.Logging
//import org.apache.spark.serializer.KryoRegistrator
//import org.apache.spark.sql.SparkSession
//
//import scala.concurrent.duration.FiniteDuration

//trait SparkSessionProvider extends Logging {
//  @transient lazy val spark: SparkSession = SparkSession.getActiveSession.getOrElse({
//    val session = SparkSession.builder().getOrCreate()
//    SparkSession.setActiveSession(session)
//    session
//  })
//
//  def await(duration: FiniteDuration)(test: => Boolean): Unit = {
//    def epoch = com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis()
//
//    val timeoutEpoch = epoch + duration.toMillis
//    while (test && epoch < timeoutEpoch) Thread.sleep(1000)
//  }
//
//  def workerMemory: String = Option(com.simiacryptus.ref.wrappers.RefSystem.getenv("SPARK_WORKER_MEMORY")).getOrElse("60g")
//
//  def hiveRoot: Option[String] = Option(s3bucket).filter(!_.isEmpty).map(bucket => s"s3a://$bucket/data/")
//
//  def s3bucket: String
//
//  def sc = spark.sparkContext
//
//}

//class _KryoRegistrator extends KryoRegistrator {
//
//  override def registerClasses(kryo: Kryo) {
//
//    kryo.register(classOf[(_, _)])
//    kryo.register(classOf[::[_]])
//    kryo.register(classOf[Array[Double]])
//    kryo.register(classOf[Array[Float]])
//    kryo.register(classOf[Array[Int]])
//    kryo.register(classOf[Array[String]])
//    kryo.register(classOf[Array[scala.collection.Seq[Any]]])
//    kryo.register(classOf[Array[scala.Tuple2[Any, Any]]])
//    kryo.register(classOf[Array[scala.Tuple3[Any, Any, Any]]])
//    kryo.register(classOf[java.lang.Class[Any]])
//    kryo.register(classOf[java.util.TreeMap[Any, Any]])
//    kryo.register(classOf[java.util.HashMap[Any, Any]])
//    kryo.register(classOf[java.util.HashSet[Any]])
//    kryo.register(classOf[java.util.LinkedHashMap[Any, Any]])
//    kryo.register(classOf[java.util.LinkedHashSet[Any]])
//    kryo.register(classOf[org.apache.hadoop.io.BytesWritable])
//    kryo.register(classOf[Array[org.apache.hadoop.io.BytesWritable]])
//    kryo.register(classOf[org.codehaus.jackson.node.BooleanNode])
//    kryo.register(classOf[org.codehaus.jackson.node.IntNode])
//    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
//    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
//    kryo.register(classOf[scala.collection.Seq[Any]])
//    kryo.register(classOf[scala.Tuple3[Any, Any, Any]])
//    kryo.register(scala.collection.immutable.Nil.getClass)
//    kryo.register(scala.math.Ordering.Double.getClass)
//    kryo.register(scala.math.Ordering.Float.getClass)
//    kryo.register(scala.math.Ordering.Int.getClass)
//    kryo.register(scala.math.Ordering.Long.getClass)
//    kryo.register(scala.None.getClass)
//    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
//    kryo.register(classOf[Array[org.apache.spark.sql.types.StructType]])
//    kryo.register(classOf[Array[org.apache.spark.sql.types.StringType]])
//    kryo.register(classOf[Array[org.apache.spark.sql.types.IntegerType]])
//    kryo.register(classOf[Array[org.apache.spark.sql.types.DoubleType]])
//    kryo.register(classOf[Array[org.apache.spark.sql.types.ArrayType]])
//    kryo.register(classOf[org.apache.spark.sql.types.StructField])
//    kryo.register(classOf[org.apache.spark.sql.types.StructType])
//    kryo.register(classOf[org.apache.spark.sql.types.StringType])
//    kryo.register(classOf[org.apache.spark.sql.types.IntegerType])
//    kryo.register(classOf[org.apache.spark.sql.types.DoubleType])
//    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
//  }
//}
