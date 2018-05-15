package com.aliyun.emr.example.flink.benchmark

import org.apache.flink.api.java.tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowedWordCount extends AbstractStreaming[(String, Int)] {
  override def execute(value: DataStream[tuple.Tuple2[String, String]]): DataStream[(String, Int)] = {
    val result = value.flatMap(record => record.f1.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(config.getProperty("flink.window.length.second").toLong))
      .sum(1)
    result
  }

  def main(args: Array[String]): Unit = {
    runJob(args)
  }
}
