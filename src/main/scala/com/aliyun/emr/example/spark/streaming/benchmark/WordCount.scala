package com.aliyun.emr.example.spark.streaming.benchmark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream

object WordCount extends AbstractStreaming {
  override def execute(stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    stream.flatMap(kv => {
      val eventTime = kv.key().toLong
      val values = kv.value().split(" ")
      val value = new Array[(String, (Integer, Long))](values.length)
      for ( i <- 0 until values.length) {
        value(i) = (values(i), (1, eventTime))
      }
      value
    }).reduceByKey((x,y) =>{
      val count = x._1 + y._1
      var eventTime = x._2
      if (x._2 < y._2) {
        eventTime = y._2
      }
      (count, eventTime)
    }).map(x => x._2._2 + "," + System.currentTimeMillis())
      .saveAsTextFiles(config.getProperty("filename.prefix") + config.getProperty("name") + "/result")
  }

  def main(args: Array[String]): Unit = {
    runJob(args)
  }
}
