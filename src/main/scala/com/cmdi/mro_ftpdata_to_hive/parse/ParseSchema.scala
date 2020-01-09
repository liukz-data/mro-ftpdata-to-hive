package com.cmdi.mro_ftpdata_to_hive.parse

import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
  * 此对象用来构建spark入库所需schema
  */
object ParseSchema {

   def parseToSparkShcema(parseJson: ParseJson):StructType={
    val schema:Array[StructField]=new Array[StructField](parseJson.nameArr.size+3)
     var i = 0
     parseJson.nameMold.foreach(nameMold=>{
       val fieldName = nameMold._1
       val fieldTypeStr = nameMold._2
       val fieldType:DataType = fieldTypeStr match {
         case "string" => DataTypes.StringType
         case "bigint" => DataTypes.LongType
         case "byte" => DataTypes.ByteType
         case "short" => DataTypes.ShortType
         case "int" => DataTypes.IntegerType
         case _ => DataTypes.NullType
       }
       schema(i) = StructField(fieldName,fieldType)
       i=i+1
     })

     schema(i) = StructField("city",DataTypes.StringType)
     schema(i+1) = StructField("logdate",DataTypes.StringType)
     schema(i+2) = StructField("hour",DataTypes.StringType)
     StructType(schema)
  }
}
