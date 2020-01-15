package com.cmdi.mro_ftpdata_to_hive.load

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.cmdi.mro_ftpdata_to_hive.bean.FTPFileInfo
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientPool
import com.cmdi.mro_ftpdata_to_hive.parse.{ParseJson, ParseSchema, ParseXml}
import com.cmdi.mro_ftpdata_to_hive.submit.FTPCompressedFileToHiveHelper
import com.cmdi.mro_ftpdata_to_hive.util.{MroFieldTypeConvertUtil, PgMergeLogicTb, PropertiesUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 此对象中方法为主要的xml解析和入库调度逻辑
  */
object LoadDataToHive {
  var spark: SparkSession = _
  var sc: SparkContext = _
  var ftpClientPoolBroadcast: Broadcast[FTPClientPool] = _
  var logDate: String = _
  var fs: FileSystem = _
  //mro-ftpdata-to-hive-conf.properties配置文件
  var appConfPro: Properties = _
  //city_date_hour.properties文件路径
  var parseJsonBroadcast:Broadcast[ParseJson] = _
  var mroFieldTypeConvertUtilBrocast:Broadcast[MroFieldTypeConvertUtil] = _
  private var city_date_hour_conf_file_path: String = _
  private val sm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  //将所需属性通过此方法放入此对象中
  def putLoadDataToHiveNeccessaryPros(spark: SparkSession, sc: SparkContext, ftpClientPoolBroadcast: Broadcast[FTPClientPool], logDate: String, fs: FileSystem, appConfPro: Properties,parseJsonBroadcast:Broadcast[ParseJson],mroFieldTypeConvertUtilBrocast:Broadcast[MroFieldTypeConvertUtil]): Unit = {
    LoadDataToHive.spark = spark
    LoadDataToHive.sc = sc
    LoadDataToHive.ftpClientPoolBroadcast = ftpClientPoolBroadcast
    LoadDataToHive.logDate = logDate
    LoadDataToHive.fs = fs
    LoadDataToHive.appConfPro = appConfPro
    city_date_hour_conf_file_path = appConfPro.getProperty("city_date_hour_conf_file_path")
    LoadDataToHive.parseJsonBroadcast = parseJsonBroadcast
    LoadDataToHive.mroFieldTypeConvertUtilBrocast = mroFieldTypeConvertUtilBrocast
  }

  /**
    * 从ftp下载压缩文件，解析其中xml，然后入hive库
    *
    * @param collatingFileRdd 整理完文件所得到的rdd
    */
  def loadDataToHive(ftpFileInfoArr: Array[FTPFileInfo]): Unit = {
    val merge_hive_files = appConfPro.getProperty("merge_hive_files").toInt
    val ftpClientPoolBroadcast = LoadDataToHive.ftpClientPoolBroadcast
    val parseJsonBroadcast = LoadDataToHive.parseJsonBroadcast
    val mroFieldTypeConvertUtilBrocast = LoadDataToHive.mroFieldTypeConvertUtilBrocast
    val ftp_conf= appConfPro.getProperty("ftp_conf")
    val ftp_conf_pro = PropertiesUtil.getDiskProperties(ftp_conf)
    val ftp_clientpool_size_executor = ftp_conf_pro.getProperty("ftp_clientpool_size_executor").toInt
    val appConfProBroadcast = sc.broadcast[Properties](appConfPro)
     val cityHourArrFtpFileInfo =  sc.parallelize(ftpFileInfoArr).map(ftpFileInfo => {
        //以city_hour为key，ArrayBuffer(ftpFileInfo)为value
        (ftpFileInfo.getCityName + "_" + ftpFileInfo.getFileHour, ArrayBuffer(ftpFileInfo))
      }).reduceByKey((ftpFileInfo1, ftpFileInfo2) => {
      ftpFileInfo1 ++= ftpFileInfo2
    }).collect()
    cityHourArrFtpFileInfo.par.foreach(hourFtpFileInfo => {
      val pro = PropertiesUtil.getDiskProperties(city_date_hour_conf_file_path)
      val taskid = pro.getProperty("taskid").toInt
      val cityNameHour = hourFtpFileInfo._1.split("_")
      val cityName = cityNameHour(0)
      val hour = cityNameHour(1)
      val ftpFileInfo = hourFtpFileInfo._2
      println(sm.format(new Date()) + s"开始执行cityName=$cityName logDate=$logDate hour=$hour 的xml解析逻辑和入库程序")
     PgMergeLogicTb.insert(taskid, cityName, logDate, hour, "merge", "start")

     // println("ftpFileInfo:"+ftpFileInfo.toBuffer)
      //此处ftpClientPool关闭未实现
      val dataRdd = sc.parallelize(ftpFileInfo).mapPartitions{ partitions => {

        val appConfPro = appConfProBroadcast.value
        FTPCompressedFileToHiveHelper.appConfPro = appConfPro
        FTPCompressedFileToHiveHelper.initPg2()
        val ftpClientPool = ftpClientPoolBroadcast.value
        ftpClientPool.initPool(ftp_clientpool_size_executor)
        val parseJson = parseJsonBroadcast.value
        val nameMoldToMap = parseJson.nameMold.toMap
        val mroFieldTypeConvertUtil = mroFieldTypeConvertUtilBrocast.value
        val partitionMap = partitions.map(ftpFileInfo => {
          //解析xml
          ParseXml.parseXml(ftpClientPool, ftpFileInfo,parseJson,mroFieldTypeConvertUtil,nameMoldToMap)
        })
       // ftpClientPool.close()
        partitionMap
      }
     }.flatMap(line => line).map(line => {
        Row.fromSeq(line.toSeq)
      })
      val dbname = appConfPro.getProperty("dbname")
      val tb_name = appConfPro.getProperty("tb_name")
      val tmpView = String.join("_", cityName, logDate, hour)
      val structType: StructType = ParseSchema.parseToSparkShcema(parseJsonBroadcast.value)

      val dfData = spark.createDataFrame(dataRdd, structType)
      dfData.createTempView(tmpView)

      spark.sql(s"select*from $tmpView").coalesce(merge_hive_files).write.format("orc").mode(SaveMode.Overwrite).insertInto(s"$dbname.$tb_name")

      val city_date_hour_hdfs_path = appConfPro.getProperty("city_date_hour_hdfs_path")
      //生成city_date_hour文件
      fs.copyFromLocalFile(new Path(city_date_hour_conf_file_path), new Path(String.join("/", city_date_hour_hdfs_path, tmpView)))
      PgMergeLogicTb.insert(taskid, cityName, logDate, hour, "merge", "stop")
      println(sm.format(new Date()) + s"完成执行cityName=$cityName logDate=$logDate hour=$hour 的xml解析逻辑和入库程序")

    })
  }
}
