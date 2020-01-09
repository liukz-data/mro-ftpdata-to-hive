package com.cmdi.mro_ftpdata_to_hive.util

import com.cmdi.mro_ftpdata_to_hive.parse.{ParseJson, ParseXml}
import com.cmdi.mro_ftpdata_to_hive.submit.FTPCompressedFileToHiveHelper

import scala.collection.JavaConverters._

object TestParseXml {

  def main(args: Array[String]): Unit = {
    executeFTPCompressedFileToHive(args)
  }

  def executeFTPCompressedFileToHive(args: Array[String]): Unit = {


    val jsonFilePath = "G:\\Users\\lkz\\IdeaProjects\\mro-ftpdata-to-hive\\conf\\mro_field.json"
    val parseJson = new ParseJson()
    parseJson.parseJson(jsonFilePath)

    val ftpClientPool = FTPCompressedFileToHiveHelper.initFTPClientPool(true) //获得ftp连接池

    val ftpClient = ftpClientPool.borrowObject()

    val ftpFiles = new FTPFindCompressedFileUtil().getAllCompressedFile(ftpClient, "/20181211_1/104211", ".*20181211.*")

    val mroFieldTypeConvertUtil = new MroFieldTypeConvertUtil
    mroFieldTypeConvertUtil.convertInit()
    //此处ftpClientPool关闭未实现
   ftpFiles.asScala.map(ftpFileInfo=>{
      ParseXml.parseXml(ftpClientPool,ftpFileInfo,parseJson,mroFieldTypeConvertUtil,parseJson.nameMold.toMap)
    })

  }
}
