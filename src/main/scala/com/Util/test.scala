package com.Util

import org.apache.spark.sql.SparkSession

object test {

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName("sad")
      .master("local")
      .getOrCreate()

    val arr = Array("https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=3bbb65014e2c58c5a114fefa00c0df26&radius=1000&extensions=all")

    val unit = session.sparkContext.makeRDD(arr)

    unit.map{
      x=>
        HttpUtil.get(x)
    }.foreach(println)
  }

}
