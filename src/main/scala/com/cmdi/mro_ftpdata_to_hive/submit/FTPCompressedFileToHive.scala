package com.cmdi.mro_ftpdata_to_hive.submit

import java.text.SimpleDateFormat
import java.util.Date

import com.cmdi.mro_ftpdata_to_hive.ftp.{FTPClientPool, FTPClientUtil}
import com.cmdi.mro_ftpdata_to_hive.load.LoadDataToHive
import com.cmdi.mro_ftpdata_to_hive.parse._
import com.cmdi.mro_ftpdata_to_hive.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._


/**
  * ftp文件整理和过滤,已完成，未进行服务器测
  * xml解析逻辑，已完成，同上
  * 服务器本地测试，未完成
  * 建测试表,已完成，同上
  * 将数据分小时，已完成，同上
  * 配置文件完善，已完成，同行
  * 日志模块，未完成
  * city_date_hour文件输出模块,已完成
  * 代码规范化整理，未完成
  * 错误处理逻辑，未完成
  */
object FTPCompressedFileToHive {

  private var hadoopConf: Configuration = _
  private val sm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def main(args: Array[String]): Unit = {
    executeFTPCompressedFileToHive(args)
  }

  def executeFTPCompressedFileToHive(args: Array[String], hadoopConfMap: Map[String, String] = null): Unit = {
    println(sm.format(new Date()) + "INFO FTPCompressedFileToHive:开始执行mro解析xml及入hive库程序")
    val argsLen = args.length
    var confPath = ".\\conf\\mro-ftpdata-to-hive-conf.properties"
    var logDate = "20181211"
    var matchStr = "20181211"
    if (argsLen < 2) {
      System.err.println("使用方法：com.cmdi.mro_ftpdata_to_hive.submit.FTPCompressedFileToHive <配置文件路径 日期/日期小时>")
      System.exit(1)
    } else {
      confPath = args(0)
      matchStr = args(1)
      if (matchStr.matches("^20\\d{6}")) {
        logDate = matchStr
      } else if (matchStr.matches("^20\\d{8}")) {
        logDate = matchStr.substring(0, 8)
      } else {
        System.out.println("日期/日期小时格式输入错误：" + matchStr)
        System.exit(1)
      }
    }

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
    //加载配置文件
    val confPro = PropertiesUtil.getDiskProperties(confPath)
    FTPCompressedFileToHiveHelper.appConfPro = confPro
    FTPCompressedFileToHiveHelper.initPg()

    val log_level = confPro.getProperty("log_level")
    val comp_merge = confPro.getProperty("comp_merge")
    val mroobj_json = confPro.getProperty("mroobj_json")
    val jsonFilePath = mroobj_json
    Logger.getLogger("org").setLevel(Level.toLevel(log_level.toUpperCase(), Level.INFO))
    Logger.getLogger("hive").setLevel(Level.toLevel(log_level.toUpperCase(), Level.INFO))
    Logger.getLogger("com.cmdi").setLevel(Level.toLevel(log_level.toUpperCase(), Level.INFO))

    val parseJson = new ParseJson()
    parseJson.parseJson(jsonFilePath)
    val mroFieldTypeConvertUtil = new MroFieldTypeConvertUtil
    mroFieldTypeConvertUtil.convertInit()
    val sconf = new SparkConf().setAppName("FTPCompressedFileToHive")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
    sconf.registerKryoClasses(Array(classOf[FTPClientUtil]))
    if (hadoopConfMap != null) {
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item => {
        sconf.set(item._1, item._2)
      })
      // 本地测试用，设置 Master，默认为 local[2]
      sconf.setMaster("local[2]")
    }

    val spark = SparkSession.builder().config(sconf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val ftpClientPoolDriver = FTPCompressedFileToHiveHelper.initFTPClientPool(true)
    val ftpClientPoolExecutor = FTPCompressedFileToHiveHelper.initFTPClientPool(false)
    val ftpClientPoolBroadcast = sc.broadcast[FTPClientPool](ftpClientPoolExecutor) //将ftp连接池广播出去
    val parseJsonBroadcast = sc.broadcast[ParseJson](parseJson)
    val mroFieldTypeConvertUtilBrocast = sc.broadcast[MroFieldTypeConvertUtil](mroFieldTypeConvertUtil)
    val ftpClient = ftpClientPoolDriver.borrowObject()
    println(sm.format(new Date()) + "INFO FTPCompressedFileToHive:开始查找所有符合条件的ftp文件，匹配条件为：" + matchStr)
    //得到所有ftp上所有符合匹配条件的，ftp文件
    val ftpFiles = new FTPFindCompressedFileUtil().getAllCompressedFile(ftpClient, ftpClient.printWorkingDirectory(), s".*$matchStr.*").asScala
    println(sm.format(new Date()) + "INFO FTPCompressedFileToHive:完成查找所有符合条件的ftp文件，匹配条件为：" + matchStr)

    println(sm.format(new Date()) + "INFO FTPCompressedFileToHive:开始在ftp创建“省/日期/地市/基站id”文件夹和移动ftp文件")
    //整理文件所需要的属性信息
    FTPDirCollating.putNecessaryPros(spark, sc, ftpClientPoolDriver, ftpClientPoolBroadcast, ftpFiles, confPro, logDate)
    val collatingFileRdd = FTPDirCollating.collatingFileAndGetRdd //整理文件和得到下一步所需要的rdd
    println(sm.format(new Date()) + "INFO FTPCompressedFileToHive:完成在ftp创建“省/日期/地市/基站id”文件夹和移动ftp文件")
    ftpClientPoolDriver.close()
    val fs = FileSystem.get(hadoopConf)

    //主调度逻辑，下载ftp上数据，放入hive
    LoadDataToHive.putLoadDataToHiveNeccessaryPros(spark, sc, ftpClientPoolBroadcast, logDate, fs, confPro,parseJsonBroadcast,mroFieldTypeConvertUtilBrocast)
    LoadDataToHive.loadDataToHive(collatingFileRdd)

    spark.stop()
    println(sm.format(new Date()) + "INFO FTPCompressedFileToHive:结束执行mro解析xml及入hive库程序")
  }


}
