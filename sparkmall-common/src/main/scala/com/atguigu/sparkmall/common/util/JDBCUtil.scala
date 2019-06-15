package com.atguigu.sparkmall.common.util

import java.util.Properties

import com.alibaba.druid.pool.{DruidAbstractDataSource, DruidDataSourceFactory}

object JDBCUtil {

  val dataSource = initConnection()

  /**
    * 初始化连接
    */
  def initConnection()={
    val properties = new Properties()
    //获取配置文件中的mysql数据库连接
    val config = ConfigurationUtil("config.properties")
    properties.setProperty("driverClassName","com.mysql.jdbc.Driver")
    properties.setProperty("url",config.getString("jdbc.url"))
    properties.setProperty("username",config.getString("jdbc.user"))
    properties.setProperty("password",config.getString("jdbc.password"))
    properties.setProperty("maxActive",config.getString("jdbc.maxActive"))

    //获取连接池对象
     DruidDataSourceFactory.createDataSource(properties)

  }

  /**
    * 执行单条sql语句：insert into table values(?,?,?)
    */
  def executeUpdate(sql:String,args:Array[Any]): Unit ={
      val conn = dataSource.getConnection
      conn.setAutoCommit(false)
      val ps = conn.prepareStatement(sql)
      if(args!=null && args.length>0){
        (0 until args.length ).foreach{
          i => ps.setObject(i+1,args(i))
        }
      }
      ps.execute()
      conn.commit()
  }

  /**
    * 批处理sql
    */

  def executeBatchUpdate(sql:String,argsList:Iterable[Array[Any]]): Unit ={
      val conn = dataSource.getConnection
      conn.setAutoCommit(false)
      val ps = conn.prepareStatement(sql)
      argsList.foreach{
        case args : Array[Any] =>{
          (0 until args.length).foreach{
            i => ps.setObject(i+1,args(i))
          }
          ps.addBatch()
        }
      }
    ps.executeBatch()
    conn.commit()
  }
}
