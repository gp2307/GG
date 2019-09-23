package com.Location

import com.Util.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
  * 媒体分析指标
  */

class APP{}

object APP {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输入目录有误")
      sys.exit()
    }

    val Array(inputpath,docs) = args

    val session = SparkSession.builder()
      .appName("das")
      .master("local[*]")
      .getOrCreate()

    //读取字典数据
    val docMap = session.sparkContext.textFile(docs).map(_.split("\\s", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    //进行广播
    val broadcast = session.sparkContext.broadcast(docMap)

    //读取数据文件
    val df = session.read.parquet(inputpath)

    df.rdd.map(row=>{
      // 取媒体相关字段
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 处理请求数
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      // 处理展示点击
      val clickList = RptUtils.clickPt(requestmode,iseffective)
      // 处理广告
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      // 所有指标
      val allList:List[Double] = rptList ++ clickList ++ adList
      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      // list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))

      //      .saveAsTextFile(outputPath)
      .foreach(println)

  }

}
