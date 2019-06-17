package com.atguigu.sparkmall.offline.app

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.offline.bean.Condition
import org.apache.spark.sql.SparkSession

object OfflineApp {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder()
      .appName("offlineApp")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .enableHiveSupport().getOrCreate()

    val taskId = UUID.randomUUID().toString
    // 根据条件过滤取出需要的 RDD, 过滤条件定义在配置文件中
    val userVisitActionRDD  = readUserVisitActionRDD(spark,readConditions)
    userVisitActionRDD.cache() //做缓存
    /* println("任务1: 开始")
    val categoryTop10 = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)
    println("任务1: 结束")
    println("任务2: 开始")
    CategorySessionApp.statCategoryTop10Session(spark,categoryTop10,userVisitActionRDD,taskId)
    println("任务2: 结束")*/
    println("任务3:开始")
    PageConversionApp.calcPageConversion(spark,userVisitActionRDD,readConditions.targetPageFlow,taskId)
    println("任务3:结束")

   /* println("任务4: 开始")
    AreaProductClick.statAreaClickTop3Product(spark,taskId)
    println("任务4: 结束")*/




  }

  /**
    * 获取过滤后的用户行为RDD
    */
  def readUserVisitActionRDD(spark:SparkSession,condition:Condition)={
    //select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1 and v.date >=condition.startDate and v.date <= condition.endDate
    var sql=
      s"""
         |select v.*
         |from user_visit_action v
         |join user_info u on v.user_id = u.user_id where 1=1
       """.stripMargin
    if(isNotEmpty(condition.startDate)){
      sql +=s" and v.date>='${condition.startDate}'"

    }
    if(isNotEmpty(condition.endDate)){
      sql +=s" and v.date<='${condition.endDate}'"
    }
    if(condition.startAge!=0){
      sql +=s" and u.age>=${condition.startAge}"
    }
    if(condition.endAge!=0){
      sql +=s" and u.age<=${condition.endAge}"
    }
    import spark.implicits._
    spark.sql("use sparkmall")
    spark.sql(sql).as[UserVisitAction].rdd

  }

  /**
    * 读取过滤条件并封装
    */
  def readConditions:Condition={
    val config = ConfigurationUtil("conditions.properties")
    val conditionString = config.getString("condition.params.json")
    JSON.parseObject(conditionString,classOf[Condition])
  }
}
