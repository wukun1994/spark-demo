package com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.acc.MapAccumulator
import com.atguigu.sparkmall.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTop10App {

  def statCategoryTop10(spark:SparkSession,userVisitActionRDD:RDD[UserVisitAction],taskId:String)={

   // 1、注册累加器
    val acc = new MapAccumulator
    spark.sparkContext.register(acc)

    //2、遍历日志
    userVisitActionRDD.foreach{
      visitAction =>{
        if(visitAction.click_category_id != -1){
          acc.add(visitAction.click_category_id.toString,"click")
        }else if(visitAction.order_category_ids != null){
          visitAction.order_category_ids.split(",").foreach {
            oid => acc.add(oid, "order")
          }
        }else if(visitAction.pay_category_ids != null){
          visitAction.pay_category_ids.split(",").foreach {
            pid => acc.add(pid, "pay")
        }
      }
    }
  }
    // 3. 遍历完成之后就得到每个每个品类 id 和操作类型的数量.  然后按照 CategoryId 进行进行分组
    val actionCountByCategoryIdMap = acc.value.groupBy(_._1._1)
    // 4. 聚合成 CategoryCountInfo 类型的集合
    val ategoryCountInfoList =actionCountByCategoryIdMap.map{
      case(cid,actionMap)=>CategoryCountInfo(
        taskId,
        cid,
        actionMap.getOrElse((cid, "click"), 0),
        actionMap.getOrElse((cid, "order"), 0),
        actionMap.getOrElse((cid, "pay"), 0)
      )
    }.toList
    // 5. 按照 点击 下单 支付 的顺序降序来排序
    val sortedCategoryInfoList = ategoryCountInfoList.sortBy(info=>
      (info.clickCount,info.orderCount, info.payCount))(Ordering.Tuple3(
        Ordering.Long.reverse,Ordering.Long.reverse, Ordering.Long.reverse
      ))
    //6. 截取前十
    val top10 = sortedCategoryInfoList.take(10)
    //7. 插入数据库
    val argsList =top10.map(info=>Array(info.taskId, info.categoryId, info.clickCount, info.orderCount, info.payCount))
    JDBCUtil.executeBatchUpdate("insert into category_top10 values(?, ?, ?, ?, ?)", argsList)
    top10
  }



}
