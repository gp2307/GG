package com.Util

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {
  /**
    * 解析经纬度
    * @param long
    * @param lat
    * @return
    */
  def getBusinessFromAmap(long:Double,lat:Double):String = {
    // https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957
    // &key=3bbb65014e2c58c5a114fefa00c0df26&radius=1000&extensions=all
    val location = long + "," + lat
    //获取URL
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=3bbb65014e2c58c5a114fefa00c0df26"
    //调用接口发送请求
    val jsonstr = HttpUtil.get(url)
    //解析json
    val JSONObject = JSON.parseObject(jsonstr)
    //判断当前状态是否为 1
    val status = JSONObject.getIntValue("status")
    if(status == 0) return ""
    //如果不为空
    val jsonObject2 = JSONObject.getJSONObject("regeocode")
    if(jsonObject2 == null) return ""
    val jsonObject3 = jsonObject2.getJSONObject("addressComponent")
    if(jsonObject3 == null) return ""
    val jsonArray = jsonObject3.getJSONArray("businessAreas")
    if(jsonArray == null) return ""

    //定义集合取值
    val result = collection.mutable.ListBuffer[String]()
    //循环数组
    for(itme <- jsonArray.toArray()){
      if(itme.isInstanceOf[JSONObject]){
       val json = itme.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }
}
