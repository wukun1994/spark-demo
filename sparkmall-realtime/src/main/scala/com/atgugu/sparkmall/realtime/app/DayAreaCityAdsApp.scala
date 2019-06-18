package com.atgugu.sparkmall.realtime.app

import com.atgugu.sparkmall.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DayAreaCityAdsApp {

  def statAreaCityAdsCountPerDay(adsInfoDStream:DStream[AdsInfo],spark:SparkContext)={

    //统计数据
    spark.setCheckpointDir("./ck")
    val resultDSteam =adsInfoDStream.map(adsInfo=>{
      (s"${adsInfo.dayString}:${adsInfo.area}:${adsInfo.city}:${adsInfo.adsId}", 1L)
    }).reduceByKey(_+_).updateStateByKey((seq:Seq[Long],opt:Option[Long])=>{
      Some(seq.sum + opt.getOrElse(0L))
    })
    resultDSteam.foreachRDD(rdd=>{
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val totalCountArr: Array[(String, Long)] = rdd.collect
      totalCountArr.foreach{
        case(field,count) => jedisClient.hset("day:area:city:adsCount", field, count.toString)
      }
      jedisClient.close()
    })
    resultDSteam
  }
}
