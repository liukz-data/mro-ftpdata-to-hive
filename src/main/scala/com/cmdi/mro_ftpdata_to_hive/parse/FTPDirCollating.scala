package com.cmdi.mro_ftpdata_to_hive.parse

import java.io.IOException
import java.util.Properties

import com.cmdi.mro_ftpdata_to_hive.bean.FTPFileInfo
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientPool
import com.cmdi.mro_ftpdata_to_hive.util.{PgEnbidCityTb, PropertiesUtil}
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * 此对象作用是整理ftp上文件结构，将ftp上符合条件数据放入“/省/日期/地市/基站id”文件夹结构的目录中
  */
object FTPDirCollating {
  private val logger = LoggerFactory.getLogger(FTPDirCollating.getClass)
  var ftpClientPool: FTPClientPool = _
  var ftpFiles: mutable.Buffer[FTPFileInfo] = _
  var appConfPro: Properties = _
  var fileDate: String = _
  private var enbidCitynameMap:mutable.HashMap[Int, String] = _
  private val enbidCitynameMapNeed = new mutable.HashMap[Int, String]()
  private var provinceDateStr: String = _


  def putNecessaryPros(ftpClientPool: FTPClientPool, ftpFiles: mutable.Buffer[FTPFileInfo], appConfPro: Properties, fileDate: String): Unit = {
    FTPDirCollating.ftpClientPool = ftpClientPool
    FTPDirCollating.ftpFiles = ftpFiles
    FTPDirCollating.appConfPro = appConfPro
    FTPDirCollating.fileDate = fileDate

  }

  /**
    * 目的是串联执行本对象中整理文件的方法
    *
    * @return RDD[FTPFileInfo]
    */
  def collatingFileAndGetRdd: Array[FTPFileInfo] = {
    selEnbidCityNameMap()
    createProvinceDateCitynameEnbid()
    mvFTPFileAndGetCollatingFileRdd()
  }

  /**
    * 从hive对应表中查询出所需enbid和cityname的对应关系，将其放入enbidCitynameMap中
    */
  private def selEnbidCityNameMap(): Unit = {
    enbidCitynameMap = PgEnbidCityTb.selectEnbidCityname()
    PgEnbidCityTb.end()
     }


  /**
    * 创建 “/省/日期/地市/enbid” 文件夹
    */
  private def createProvinceDateCitynameEnbid(): Unit = {
    val ftpClient = ftpClientPool.borrowObject()
    val province_name = appConfPro.getProperty("province_name")
    val ftp_dir_collating_source_dir = appConfPro.getProperty("ftp_dir_collating_source_dir")
     ftpFiles.map(ftpFileInto => ftpFileInto.getEnbid).distinct.foreach(enbid => {
      val cityName = enbidCitynameMap.getOrElse(enbid,null)
      if (cityName != null) {
        enbidCitynameMapNeed +=  (enbid -> cityName)
      }
    })
    val provinceStr = String.join("/", ftp_dir_collating_source_dir, province_name)
    ftpClient.makeDirectory(provinceStr)
    provinceDateStr = String.join("/", provinceStr, fileDate)
    ftpClient.makeDirectory(provinceDateStr)
    ftpClient.close()
    enbidCitynameMapNeed.par.foreach(f = enbidCityNames => {
      val enbid = enbidCityNames._1
      val cityName = enbidCityNames._2

      val ftpClient = ftpClientPool.borrowObject()
      val provinceDateCityName = String.join("/", provinceDateStr, cityName)
      //provinceDateCityName = new String(provinceDateCityName.getBytes("UTF-8"),"iso-8859-1")
     // println(provinceDateCityName)
      ftpClient.makeDirectory(provinceDateCityName)
      val provinceDateCityNameEnbid = String.join("/", provinceDateCityName, enbid.toString)
      ftpClient.makeDirectory(provinceDateCityNameEnbid)
      ftpClient.close()
    })

   }


  /**
    * 将文件移动到对应文件夹下
    *
    * @return RDD[FTPFileInfo]
    */
  private def mvFTPFileAndGetCollatingFileRdd(): Array[FTPFileInfo] = {
    val ftp_conf= appConfPro.getProperty("ftp_conf")
    val ftp_conf_pro = PropertiesUtil.getDiskProperties(ftp_conf)
    val ftp_clientpool_size_executor = ftp_conf_pro.getProperty("ftp_clientpool_size_executor").toInt
    val ftpFileInfoArr = ftpFiles.par.map(ftpFileInfo => {

      val fileAbsolutePathOld = ftpFileInfo.getAbsolutePath
      val enbid = ftpFileInfo.getEnbid
      val cityName = enbidCitynameMapNeed.getOrElse(enbid,null)
      if (cityName != null) {

        val partentPathNew = String.join("/", provinceDateStr, cityName, enbid.toString)
        //partentPathNew = new String(partentPathNew.getBytes("UTF-8"),"iso-8859-1")
        val fileAbsolutePathNew = String.join("/", partentPathNew, ftpFileInfo.getFileName)
        ftpFileInfo.setAbsolutePath(fileAbsolutePathNew)
        ftpFileInfo.setParentPath(partentPathNew)
        ftpFileInfo.setExistEnbid(true)
        ftpFileInfo.setCityName(cityName)

        val ftpClient = ftpClientPool.borrowObject()
        if(!ftpClient.rename(fileAbsolutePathOld, fileAbsolutePathNew)) throw new IOException("FTP文件重命名失败 fileAbsolutePathOld："+fileAbsolutePathOld+" fileAbsolutePathNew:"+fileAbsolutePathNew)
        ftpClient.close()
        ftpFileInfo
      } else {
        ftpFileInfo
      }
    }).filter(ftpFileInfo => ftpFileInfo.isExistEnbid).toArray
    ftpFileInfoArr

  }
}
