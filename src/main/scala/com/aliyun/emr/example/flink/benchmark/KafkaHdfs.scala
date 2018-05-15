package com.aliyun.emr.example.flink.benchmark

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.scala._

object KafkaHdfs extends AbstractStreaming[String] {
  def main(args: Array[String]): Unit = {
    runJob(args)
  }

  override def execute(value: DataStream[Tuple2[String, String]]): DataStream[String] = {
    val result = value.map(record => record.f0 + "," + System.currentTimeMillis())
    result
  }
}

