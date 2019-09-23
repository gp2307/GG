package com.Tags

import com.Util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


/**
  * 地域标签
  */
object TagsLocation extends Tag{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    //获取地域数据
    val pro = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    if(StringUtils.isNoneBlank(pro)){
      list:+=("ZP"+pro,1)
    }
    if(StringUtils.isNoneBlank(city)){
      list:+=("ZC"+city,1)
    }
    list
  }
}
