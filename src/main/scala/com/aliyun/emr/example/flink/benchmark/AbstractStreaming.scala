package com.aliyun.emr.example.flink.benchmark

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema

abstract class AbstractStreaming[T] {
  var config:Properties = _

  def runJob(args: Array[String]): Unit = {
    config  = loadConfig(args(0))
    val kafkaPartition = config.getProperty("partition.number").toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    if (config.getProperty("flink.checkpoint.enable").toBoolean) {
      env.enableCheckpointing(1000)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    }
    env.setParallelism(config.getProperty("cluster.cores.total").toInt)
    env.setBufferTimeout(config.getProperty("flink.buffer.timeout.ms").toLong)
    val kafkaParam = new Properties()
    kafkaParam.setProperty("bootstrap.servers", config.getProperty("broker.list"))
    kafkaParam.setProperty("group.id", config.getProperty("consumer.group"))
    val schema = new KafkaKeyValueSchema
    val consumer = new FlinkKafkaConsumer011(config.getProperty("topic"), schema, kafkaParam)

    val stream = env.addSource(consumer).setParallelism(kafkaPartition)
    val result = execute(stream)
    result.writeAsText("hdfs://emr-header-1:9000" + config.getProperty("filename.prefix") + config.getProperty("name"), FileSystem.WriteMode.OVERWRITE)

    env.execute(config.getProperty("name"))

  }

  def loadConfig(path: String): Properties = {
    val properties = new Properties()
    properties.load(new BufferedInputStream(new FileInputStream(path)))
    println("all configure:" + properties)
    properties
  }

  def execute(value: DataStream[Tuple2[String, String]]):DataStream[T]
}


class KafkaKeyValueSchema extends KeyedDeserializationSchema[Tuple2[String, String]] {
  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long) = new Tuple2[String, String](new String(messageKey), new String(message))

  override def isEndOfStream(nextElement: Tuple2[String, String]) = false

  override def getProducedType = new TupleTypeInfo[Tuple2[String, String]](TypeExtractor.createTypeInfo(classOf[String]), TypeExtractor.createTypeInfo(classOf[String]))
}
