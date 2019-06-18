package com.atgugu.sparkmall.realtime

import com.atgugu.sparkmall.realtime.app.BlackListApp
import com.atgugu.sparkmall.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealTimeApp {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
     val conf = new SparkConf().setAppName("RealTimeApp").setMaster("local[2]")
    // 2. 创建 SparkContext 对象
     val sc = new SparkContext(conf)
    // 3. 创建 StreamingContext
     val ssc = new StreamingContext(sc,Seconds(1))
    // 4. 得到 DStream
     val recordDStream:InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc,"ads_log")
    // 5. 为了方便后面的计算, 把消费到的字符串封装到对象中
    val adsInfoDStream:DStream[AdsInfo] = recordDStream.map {
      record =>
        val split:Array[String] = record.value.split(",")
        AdsInfo(split(0).toLong, split(1), split(2), split(3), split(4))

    }
    BlackListApp.checkUserToBlackList(adsInfoDStream)
    ssc.start()
    ssc.awaitTermination()
  }
}
