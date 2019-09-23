package com.Tags

import com.Util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object TagsAd extends Tag{

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    //获取数据类型
    val row = args(0).asInstanceOf[Row]
    //获取广告位类型和名称
    val adType = row.getAs[Int]("adspacetype")
   //获取广告类型
    adType match {
      case v if v >= 10 => list:+=("LC"+v,1)
      case v if v > 0 && v < 10 => list:+=("LC0"+v,1)
    }
    //获取广告名称
    val adName = row.getAs[String]("adspacetypename")
    if(StringUtils.isBlank(adName)){
      list:+=("LN"+adName,1)
    }
    // 渠道标签
    val channel = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+channel,1)

    list
  }
}
