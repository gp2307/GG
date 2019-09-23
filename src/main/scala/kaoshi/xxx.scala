package kaoshi

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object xxx {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dsa").setMaster("local[*]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val df = session.read.textFile("E:\\BigData1923\\json (1).txt")

    val logs = df.rdd.collect().toBuffer

    var list = List[(String,Int)]()

    for(itme <- logs){
      val json = JSON.parseObject(itme)
      val status = json.getIntValue("status")
      if(status == 0) return ""
      val jsonObject = json.getJSONObject("regeocode")
      if(jsonObject == null || jsonObject.keySet().isEmpty) return ""
      val jsonObject1 = jsonObject.getJSONArray("pois")
      if(jsonObject1 == null || jsonObject1.isEmpty) return ""
      for(item <- jsonObject1.toArray()){
        if(item.isInstanceOf[JSONObject]){
          val businesarea: String = item.asInstanceOf[JSONObject].getString("businessarea")
          list :+= (businesarea,1)
        }
      }
    }

    val res = list.filter(x=>x._1 != "[]").groupBy(x=>x._1).mapValues(x=>x.size).toList.sortBy(x=>x._2)

    res.foreach(println)

    session.stop()
  }
}



