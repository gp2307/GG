package com.Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 统计省市指标
  */
object ProCityCt {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("输入的目录不正确")
      sys.exit()
    }

    val Array(inputpath) = args

    val session = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
    val df = session.read.parquet(inputpath)
    //注册临时视图
    df.createTempView("log")
    val df2 = session.sql("select provincename,cityname,count(1) from log group by provincename,cityname")

    //分区存储json时，要加partitionBy
    df2.write.partitionBy("provincename","cityname").json("E:\\json")

//    //存入Mysq
//    //通过config配置文件依赖进行加载相关的配置信息
//    val load = ConfigFactory.load()
//    //创建Propertise对象
//    val prop = new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
//    //存储
//    df2.write.mode(SaveMode.Append)
//      .jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableNmae"),prop)

    session.stop()





  }

}
