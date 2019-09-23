package com.Location

import com.Util.RptUtils
import org.apache.spark.sql.SparkSession

/**
  * 地域分布指标
  */
object Territory {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输入的目录错误")
      sys.exit()
    }

    val Array(inputpath,outputpath) = args

    val session = SparkSession
      .builder()
      .appName("ppp")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
   val df = session.read.parquet(inputpath)
    //REQUESTMODE	PROCESSNODE	ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDERID WinPrice adpayment
    df.rdd.map{
      x=>
     val requestmode = x.getAs[Int]("requestmode")
        val processnode = x.getAs[Int]("processnode")
        val iseffective = x.getAs[Int]("iseffective")
        val isbilling = x.getAs[Int]("isbilling")
        val isbid = x.getAs[Int]("isbid")
        val iswin = x.getAs[Int]("iswin")
        val adorderid = x.getAs[Int]("adorderid")
        val winprice = x.getAs[Double]("winprice")
        val adpayment = x.getAs[Double]("adpayment")
        //处理请求数
        val reqList = RptUtils.ReqPt(requestmode,processnode)
        //处理点击、展示数
        val clickptList = RptUtils.clickPt(requestmode,iseffective)
        //处理竞价，成功成本和消费
        val adpList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)

        //拼接数组
        val allList:List[Double] = reqList ++ clickptList ++ adpList

        //最终返回
        ((x.getAs[String]("provincename"),x.getAs[String]("cityname")),allList)
    }.reduceByKey{
      (list1,list2)=>{
        list1.zip(list2).map(x=>x._1+x._2)
      }
    }.map{
      x=>
        x._1+","+x._2.mkString(",")
    }.saveAsTextFile(outputpath)

  }
}
