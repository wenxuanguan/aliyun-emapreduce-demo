package com.aliyun.emr.example.flink.benchmark

import org.apache.flink.api.java.tuple
import org.apache.flink.streaming.api.scala._

object WordCount extends AbstractStreaming[(String, Int)] {
  override def execute(value: DataStream[tuple.Tuple2[String, String]]): DataStream[(String, Int)] = {
    val result = value.flatMap(record => record.f1.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    result
  }

  def main(args: Array[String]): Unit = {
    runJob(args)
  }

}
