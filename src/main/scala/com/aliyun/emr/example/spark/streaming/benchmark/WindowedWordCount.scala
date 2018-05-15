package com.aliyun.emr.example.spark.streaming.benchmark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.InputDStream

object WindowedWordCount extends AbstractStreaming {
  override def execute(stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    val windowLength = config.getProperty("window.length.ms").toLong
    val slideInterval = config.getProperty("slide.interval.ms").toLong
    stream.flatMap[(String, (Integer, Long))](record => {
      val eventTime = record.key().toLong
      val values = record.value().split(" ")
      val result = new Array[(String, (Integer, Long))](values.length)
      for (i <- 0 until values.length) {
        result(i) = (values(i), (1, eventTime))
      }
      result
    }).reduceByKeyAndWindow((x: (Integer, Long), y:(Integer, Long)) => {
      val count: Integer = x._1 + y._1
      var eventTime = x._2
      if (y._2 > eventTime) {
        eventTime = y._2
      }
      (count, eventTime)
    }, Duration(windowLength), Duration(slideInterval)).map(x => x._2._2 + "" + System.currentTimeMillis())
      .saveAsTextFiles(config.getProperty("filename.prefix") + config.getProperty("name") + "/result")
  }

  def main(args: Array[String]): Unit = {
    runJob(args)
  }
}
