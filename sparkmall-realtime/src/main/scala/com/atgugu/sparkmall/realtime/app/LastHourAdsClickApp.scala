package com.atgugu.sparkmall.realtime.app

import java.text.SimpleDateFormat

import com.atgugu.sparkmall.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.sql.catalyst.expressions.Minute
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods

object LastHourAdsClickApp {

  def statLastHourAdsClick(filteredDStream: DStream[AdsInfo]) ={
    val windowDStream = filteredDStream.window(Minutes(60),Seconds(5))
    val groupAdsCountDStream = windowDStream.map(adsInfo=>{
      val houreMinutes  = new SimpleDateFormat("HH:mm").format(adsInfo.ts)
      ((adsInfo.adsId,houreMinutes),1)
    }).reduceByKey(_+_).map{
      case((adsId,hourMinutes),count) => (adsId, (hourMinutes, count))
    }.groupByKey
    val jsonCountDStream =groupAdsCountDStream.map{
      case(adsId,it) =>{
        import org.json4s.JsonDSL._
        val hourMinutesJson = JsonMethods.compact(JsonMethods.render(it))
        (adsId, hourMinutesJson)
      }
    }
    jsonCountDStream.foreachRDD(rdd=>{
      val result = rdd.collect
      import collection.JavaConversions._
      val client = RedisUtil.getJedisClient
      client.hmset("last_hour_ads_click", result.toMap)
      client.close()
    })
  }

}
