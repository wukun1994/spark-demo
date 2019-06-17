package com.atguigu.sparkmall.offline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConversionApp {

  /*
1. 读取到规定的页面
    例如: targetPageFlow:"1,2,3,4,5,6,7"

2. 过滤出来规定页面的日志记录 并统计出来每个页面的访问次数
    例如: 只需过滤出来1,2,3,4,5,6   第7页面不需要过滤

3. 计算页面跳转次数(肯定是按照每个 session 来统计)
    1->2  2->3 ...
    3.1 统计每个页面访问次数

4. 计算转化率
    页面跳转次数 / 页面访问次数
    1->2/1 表示页面1到页面2的转化率

5. 保存到数据库
 */

  def calcPageConversion(spark:SparkSession,userVisitActionRDD:RDD[UserVisitAction],targetPageFlow:String,taskId:String): Unit ={
    //读取到规定的页面
    val pageFlowArr  = targetPageFlow.split(",")
    //最后一个页面不需要跳转
    val prePageFlowArr  = pageFlowArr.slice(0,pageFlowArr.length-1)
    //0->1, 1->2,2->3
    val postPageFlowArr  = pageFlowArr.slice(1,pageFlowArr.length)
    // 2. 过滤出来规定页面的日志记录, 并统计出来每个页面的访问次数 countByKey 是行动算子  reduceByKey 是转换算子
    val targetPageCount  = userVisitActionRDD.filter(
      uva => pageFlowArr.contains(uva.page_id.toString)
    ).map(uva=>(uva.page_id,1L)).countByKey
    // 3. 明确哪些页面需要计算跳转次数 1-2  2-3 3-4 ...
    val targetJumpPages  = prePageFlowArr.zip(postPageFlowArr).map(
      t => t._1 + "-" + t._2
    )
    // 4. 按照 session 统计所有页面的跳转次数, 并且需要按照时间升序来排序
    // 4.1 按照 session 分组, 然后并对每组内的 UserVisitAction 进行排序
    val pageJumpRDD =userVisitActionRDD.groupBy(_.session_id).flatMap{
      case(sid,actions) =>{
        val visitActions= actions.toList.sortBy(_.action_time )
        //转换访问流水
        val pre = visitActions.slice(0, visitActions.length - 1)
        val post = visitActions.slice(1, visitActions.length)
        // 4.3 过滤出来和统计目标一致的跳转
        pre.zip(post).map(t=>t._1.page_id + "-" + t._2.page_id).filter(targetJumpPages.contains(_))
      }
    }
    // 5. 统计跳转次数  数据量已经很少了, 拉到驱动端计算
    val pageJumpCount  = pageJumpRDD.map((_,1)).reduceByKey(_ + _).collect()
    // 6. 计算跳转率
    val formatter  = new DecimalFormat(".00%")
    // 转换成百分比
    val conversionRate = pageJumpCount.map{
      case(p2p,jumpCount)  =>
        val visitCount = targetPageCount.getOrElse(p2p.split("-").head.toLong,0L)
        val rate = formatter.format(jumpCount.toDouble/visitCount)
        (p2p,rate)
    }
    val result = conversionRate.map {
      case (p2p, conversionRate) => Array(taskId.asInstanceOf[Any], p2p, conversionRate)
    }
    JDBCUtil.executeUpdate("truncate page_conversion_rate", null)
    JDBCUtil.executeBatchUpdate("insert into page_conversion_rate values(?, ?, ?)", result)
  }
}
