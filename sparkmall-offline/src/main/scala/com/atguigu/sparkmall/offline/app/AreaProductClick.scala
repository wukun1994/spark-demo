package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.offline.udf.AreaClickUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaProductClick {

  def statAreaClickTop3Product(spark:SparkSession,taskId:String)={
    //注册自定义函数
    spark.udf.register("city_remark",new AreaClickUDAF)
    spark.sql(
      """
        |select
        |	c.*,
        |	v.click_product_id,
        |	p.product_name
        |from user_visit_action v join city_info c join product_info p on v.city_id=c.city_id and v.click_product_id=p.product_id
        |where click_product_id >-1
      """.stripMargin).createOrReplaceTempView("t1")
    //2 计算每个区域每个产品的点击量
    spark.sql(
      """
        |select
        |t1.area,
        |t1.product_name,
        |count(*) click_count,
        |city_remark(t1.city_name)
        |from t1 group by t1.area,t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")
    //3、对每个区域内产品的点击量进行倒序排序
    spark.sql(
      """
        |select
        |*,
        |rank() over(partition by t2.area order by t2.click_count desc) rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")
    //取前三
    val conf = ConfigurationUtil("config.properties")
    val properties = new Properties()
    properties.setProperty("user",conf.getString("jdbc.user"))
    properties.setProperty("password",conf.getString("jdbc.password"))
    spark.sql(
      """
        |select
        |*
        |from t3 where rank <=3
      """.stripMargin).write.mode(SaveMode.Overwrite)jdbc(conf.getString("jdbc.url"),"arer_product_click",properties)
  }

}
