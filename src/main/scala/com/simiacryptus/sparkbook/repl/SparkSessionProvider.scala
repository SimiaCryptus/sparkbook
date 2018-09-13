package com.simiacryptus.sparkbook.repl

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  def s3bucket: String = null

  def hiveRoot: Option[String] = Option(s3bucket).map(bucket => s"s3a://$bucket/data/")

  @transient lazy val spark: SparkSession = {
    val builder = SparkSession.builder()
      .config("fs.s3a.aws.credentials.provider", classOf[DefaultAWSCredentialsProviderChain].getCanonicalName)
      .config("hive.default.fileformat", "Parquet")
      .config("spark.io.compression.codec", "lz4")
    //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .config("spark.kryo.registrationRequired","true")
    //      .config("spark.kryo.registrator",classOf[_KryoRegistrator].getCanonicalName)
    //      .config("spark.kryoserializer.buffer.max", "128m")
    //      .config("spark.kryoserializer.buffer", "64m")
    if (hiveRoot.isDefined) {
      builder.config("spark.sql.warehouse.dir", hiveRoot.get)
      builder.enableHiveSupport()
    }
    builder.getOrCreate()
  }

  def sc = spark.sparkContext

}

class _KryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {

    kryo.register(classOf[(_, _)])
    kryo.register(classOf[::[_]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[scala.collection.Seq[Any]]])
    kryo.register(classOf[Array[scala.Tuple2[Any, Any]]])
    kryo.register(classOf[Array[scala.Tuple3[Any, Any, Any]]])
    kryo.register(classOf[java.lang.Class[Any]])
    kryo.register(classOf[java.util.TreeMap[Any, Any]])
    kryo.register(classOf[java.util.HashMap[Any, Any]])
    kryo.register(classOf[java.util.HashSet[Any]])
    kryo.register(classOf[java.util.LinkedHashMap[Any, Any]])
    kryo.register(classOf[java.util.LinkedHashSet[Any]])
    kryo.register(classOf[org.apache.hadoop.io.BytesWritable])
    kryo.register(classOf[Array[org.apache.hadoop.io.BytesWritable]])
    kryo.register(classOf[org.codehaus.jackson.node.BooleanNode])
    kryo.register(classOf[org.codehaus.jackson.node.IntNode])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[scala.collection.Seq[Any]])
    kryo.register(classOf[scala.Tuple3[Any, Any, Any]])
    kryo.register(scala.collection.immutable.Nil.getClass)
    kryo.register(scala.math.Ordering.Double.getClass)
    kryo.register(scala.math.Ordering.Float.getClass)
    kryo.register(scala.math.Ordering.Int.getClass)
    kryo.register(scala.math.Ordering.Long.getClass)
    kryo.register(scala.None.getClass)
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructType]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StringType]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.IntegerType]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.DoubleType]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.ArrayType]])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])
    kryo.register(classOf[org.apache.spark.sql.types.StringType])
    kryo.register(classOf[org.apache.spark.sql.types.IntegerType])
    kryo.register(classOf[org.apache.spark.sql.types.DoubleType])
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
  }
}