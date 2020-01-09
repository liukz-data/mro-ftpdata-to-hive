package com.cmdi.mro_ftpdata_to_hive.submit

import java.io.FileReader
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.cmdi.mro_ftpdata_to_hive.parse.{AccessHdfsUtil, ParseJson}
import com.cmdi.mro_ftpdata_to_hive.parse.ParseJson
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 建mro表
  */

object CreateMroTable {

  private var hadoopConf:Configuration = _
  private val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    executeCreateMroTable(args)
  }

  def executeCreateMroTable(args:Array[String],hadoopConfMap:Map[String,String]=null): Unit ={

    var creTbPath = ""
    if(args.size == 1){
      creTbPath = args(0)
    }else{
      println(simpleDateFormat.format(new Date())+" ERROR:输入参数过多或无参数输入")
      return
    }

    val pro = new Properties()
    pro.load(new FileReader(creTbPath))
    val dbname = pro.getProperty("dbname")
    val tb_name = pro.getProperty("tb_name")
    val mroobj_json = pro.getProperty("mroobj_json")
    val tb_location = pro.getProperty("tb_location")
    val tb_compress = pro.getProperty("tb_compress")
    val logLevel = pro.getProperty("log_level")
    Logger.getLogger("org").setLevel(Level.toLevel(logLevel.toUpperCase(),Level.INFO))
    Logger.getLogger("hive").setLevel(Level.toLevel(logLevel.toUpperCase(),Level.INFO))
    hadoopConf = new Configuration()
    if (hadoopConfMap != null) {
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item => {
        if (item._1.startsWith("spark.hadoop.")) {
          hadoopConf.set(item._1.substring(13), item._2)
        }
      })
      AccessHdfsUtil.test1(hadoopConf)
    }

    val sconf = new SparkConf().setAppName("CreateMroTable")
    if (hadoopConfMap != null) {


      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item=>{
        sconf.set(item._1, item._2)
      })
      // 本地测试用，设置 Master，默认为 local[2]
      sconf.setMaster("local[2]")

    }

    val creTableStr = convertFieldType(mroobj_json)
    val creStrBuf= new StringBuffer()
    creStrBuf.append(s"create external table if not exists $dbname.$tb_name (")
    creStrBuf.append(creTableStr)
    creStrBuf.append(s""") partitioned by (city string,logdate string,hour string) stored as orc  location '$tb_location' tblproperties("orc.compression"="$tb_compress")""")
   println(simpleDateFormat.format(new Date())+" "+creStrBuf.toString)
    val spark = SparkSession.builder().config(sconf).enableHiveSupport().getOrCreate()
   spark.sql(creStrBuf.toString)
    spark.stop()

  }

  def convertFieldType(jsonPath:String):String={

    val parseJson = new ParseJson()
    parseJson.parseJson(jsonPath)
    val nameMolds = parseJson.nameMold

    val strBuff = new StringBuffer()
    val nameMoldsSize = nameMolds.size
    var i = 0
    nameMolds.foreach(nameMold=>{
      i = i+1
      val name = nameMold._1
      var mold = nameMold._2
      mold = mold match {
        case "byte"=>
          "tinyint"
        case "short"=>
          "smallint"
        case "int"=>
          "int"
        case "bigint"=>
          "bigint"
        case _=>
          "string"
      }


        strBuff.append(String.join(" ",name,mold)+",")

    })
    strBuff.deleteCharAt(strBuff.length()-1).toString
  }


}
