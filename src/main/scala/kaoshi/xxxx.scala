package kaoshi

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object xxxx {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("sdasa")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val df = session.read.textFile("E:\\BigData1923\\json (1).txt")

    var list = List[String]()

    val logs = df.rdd.collect().toBuffer

    for(itme <- logs){
      val json = JSON.parseObject(itme)
      val status = json.getIntValue("status")
      if(status == 0) return ""
      val regeocode = json.getJSONObject("regeocode")
      if(regeocode==null || regeocode.keySet().isEmpty) return ""
      val pois = regeocode.getJSONArray("pois")
      if(pois ==null || pois.isEmpty){
        return ""
      }
      for(item <- pois.toArray()){
        if(item.isInstanceOf[JSONObject]){
          val type1 = item.asInstanceOf[JSONObject].getString("type")
          list :+= type1
        }
      }
    }

    val res = list.flatMap(x=>x.split(";")).filter(x=>x!="").map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size).toList.sortBy(x=>x._2)

    res.foreach(println)

    session.stop()
  }
}
