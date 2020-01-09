package com.cmdi.mro_ftpdata_to_hive.parse

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}
import scala.collection.JavaConverters._

class ParseJson extends Serializable {

  //将json内的name mold值放入此map，例子enbid：int
  var nameMold: mutable.LinkedHashMap[String, String] = _
  //将将json内的name放入此数组，例子enbid，用来确定数据所放的位置
  var nameArr: ArrayBuffer[String] = _
  //nc的数量
  var nc_num:Int= _
  //json中nc的name数组
  var ncNameArr:ArrayBuffer[String] = _
  //nc数据的起始位置
  var ncDataStartIndex:Int = _
  //json中nc的mold数组
  var ncMoldArr:ArrayBuffer[String] = _
  /**
    * 解析json
    *
    * @param jsonFilePath json文件路径
    */

  def parseJson(jsonFilePath: String): Unit = {

    // val jsonFilePath = "G:\\Users\\lkz\\IdeaProjects\\mro-ftpdata-to-hive\\conf\\mro_field.json"
    var jsonReader: BufferedSource = null
    try {
      jsonReader = Source.fromFile(jsonFilePath, "UTF-8")
      val jsonStr = jsonReader.mkString
      val jsonObjectParent = JSON.parseObject(jsonStr)
      val jsonKeys = jsonObjectParent.keySet()
      val jsonObjectArray = new Array[JSONArray](jsonKeys.size())
      jsonKeys.asScala.foreach(jsonKey => {
        val jsonObjectChild = jsonObjectParent.getJSONObject(jsonKey)
        var jsonArray = jsonObjectChild.getJSONArray("fields")
        val sequence = jsonObjectChild.getString("sequence").toInt
        if (jsonKey.equals("mro_nc")) {
          ncNameArr = new ArrayBuffer[String]()
          ncMoldArr = new ArrayBuffer[String]()
          for (j <- 0 until jsonArray.size()) {
            val jsonObj = jsonArray.getJSONObject(j)
            val name = jsonObj.getString("name").toLowerCase()
            val mold = jsonObj.getString("mold").toLowerCase()
            ncNameArr += name
            ncMoldArr += mold
          }

          val nc_num = jsonObjectChild.getInteger("nc_num")
          this.nc_num = nc_num
          val jsonArrayMroNc = new JSONArray()
          for (i <- 1 to nc_num) {
            var mro_nc_num_str = i.toString
            if (nc_num <= 9) mro_nc_num_str = "0" + mro_nc_num_str
            for (j <- 0 until jsonArray.size()) {
              val jsonObj = jsonArray.getJSONObject(j)
              val mold = jsonObj.getString("mold").toLowerCase
              val name = jsonObj.getString("name").toLowerCase()+mro_nc_num_str
              val jsonObjNew = new JSONObject()
              jsonObjNew.put("mold",mold)
              jsonObjNew.put("name",name)
              jsonArrayMroNc.add(jsonObjNew)
            }
        }
          jsonArray=jsonArrayMroNc
      }
        jsonObjectArray (sequence - 1) = jsonArray
    }
    )

    nameMold = new mutable.LinkedHashMap[String, String]()
    nameArr = new ArrayBuffer[String]()
    jsonObjectArray.foreach(jsonArray => {
      for (i <- 0 until jsonArray.size()) {
        val jsonObj = jsonArray.getJSONObject(i)
        val mold = jsonObj.getString("mold").toLowerCase
        val name = jsonObj.getString("name").toLowerCase()
        nameMold.put(name, mold)
        nameArr += name
      }
    })
      ncDataStartIndex = nameArr.indexOf(ncNameArr(0)+"01")
  }

  catch
  {
    case e: Exception =>
      throw e
  }
  finally
  {
    if (jsonReader != null) jsonReader.close()
  }
}


}
