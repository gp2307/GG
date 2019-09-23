package com.Util

object RptUtils {
  //处理请求数
  def ReqPt(requestmode:Int,processnode:Int):List[Double] = {
    if(requestmode == 1 && processnode == 1){
      List[Double](1,0,0)
    }else if(requestmode == 1 && processnode == 2){
      List[Double](1,1,0)
    }else if(requestmode == 1 && processnode == 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  //处理点击展示数  clickPt
  def clickPt(requestmode:Int,iseffective:Int):List[Double] = {
    if(requestmode == 2 && iseffective== 1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective== 1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }

  //处理竞价，成功数广告成本和消费 adPt
  def adPt(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,
           winprice:Double,adpayment:Double):List[Double] = {
  if(iseffective == 1 && isbilling == 1 && isbid == 1){
  if(iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0){
  List[Double](1,1,winprice/1000,adpayment/1000)
  }else{
    List[Double](1,0,0,0)
  }
  }else{
    List[Double](0,0,0,0)
  }
  }
}
