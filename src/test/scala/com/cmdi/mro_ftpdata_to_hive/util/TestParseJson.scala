package com.cmdi.mro_ftpdata_to_hive.util

import com.cmdi.mro_ftpdata_to_hive.parse.{ParseJson, ParseSchema}

object TestParseJson {

  def main(args: Array[String]): Unit = {

    val jsonFilePath = "G:\\Users\\lkz\\IdeaProjects\\mro-ftpdata-to-hive\\conf\\mro_field.json"
    val parseJosn = new ParseJson()
    parseJosn.parseJson(jsonFilePath)
    val nameArr = parseJosn.nameArr
    println(nameArr)
    println(parseJosn.ncNameArr)
    println(parseJosn.nameMold)
    println(parseJosn.ncDataStartIndex)
    ParseSchema.parseToSparkShcema(parseJosn).printTreeString()
    val mroFieldTypeConvertUtil = new MroFieldTypeConvertUtil
    mroFieldTypeConvertUtil.convertInit()
    mroFieldTypeConvertUtil.executeConvert("bigint","1")
    mroFieldTypeConvertUtil.executeConvert("int","1")
   /* val array = Array[Array[Int]](Array(3,4,5),Array(1,2))
    array.flatMap(x=>{
      x
    }).map(x=>{
      println(x)
    })*/
  }
}
