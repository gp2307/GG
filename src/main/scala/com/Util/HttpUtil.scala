package com.Util

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils


object HttpUtil {
  /**
    * http协议
    * @param url
    * @return
    */
  def get(url:String):String ={
    //使用默认的协议
    val client = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    //获取发送请求
    val httpResponse = client.execute(httpGet)
    //处理返回结果
    EntityUtils.toString(httpResponse.getEntity,"utf-8")
  }
}
