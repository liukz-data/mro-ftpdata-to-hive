package com.cmdi.mro_ftpdata_to_hive.util

import com.cmdi.mro_ftpdata_to_hive.parse.ParseJson
import com.cmdi.mro_ftpdata_to_hive.submit.FTPCompressedFileToHiveHelper
import org.apache.commons.net.ftp.FTPFile

import scala.collection.JavaConverters._

object TestFtpFileDeal {

  def main(args: Array[String]): Unit = {
    executeFTPCompressedFileToHive(args)
  }

  def executeFTPCompressedFileToHive(args: Array[String]): Unit = {


    val jsonFilePath = "G:\\Users\\lkz\\IdeaProjects\\mro-ftpdata-to-hive\\conf\\mro_field.json"
    new ParseJson().parseJson(jsonFilePath)

    val ftpClientPool = FTPCompressedFileToHiveHelper.initFTPClientPool(true) //获得ftp连接池

    val ftpClient = ftpClientPool.borrowObject()

    val ftpFiles = new FTPFindCompressedFileUtil().getAllCompressedFile(ftpClient, "/20181211_1/104211", ".*20181211.*")


    val testPath = "/test"

    println(ftpClient.makeDirectory(testPath))
    //此处ftpClientPool关闭未实现
   ftpFiles.asScala.map(ftpFileInfo=>{
     ftpFileInfo.getEnbid
    }).distinct

  }
}
