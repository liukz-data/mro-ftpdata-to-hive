package com.cmdi.mro_ftpdata_to_hive.submit

import java.util.Properties

import com.cmdi.mro_ftpdata_to_hive.ftp._
import com.cmdi.mro_ftpdata_to_hive.util.{PgMergeLogicTb, PropertiesUtil}
import org.apache.commons.net.ftp.FTP

/**
  * FTPCompressedFileToHive的帮助类，初始化ftp连接池工具和pg连接工具
  */
object FTPCompressedFileToHiveHelper {

  var appConfPro:Properties = _

  def initFTPClientPool(isDriver:Boolean): FTPClientPool={
    val ftp_conf= appConfPro.getProperty("ftp_conf")
    val ftp_conf_pro = PropertiesUtil.getDiskProperties(ftp_conf)
    val ftp_hostname = ftp_conf_pro.getProperty("ftp_hostname")
    val ftp_port = ftp_conf_pro.getProperty("ftp_port").toInt
    val ftp_username = ftp_conf_pro.getProperty("ftp_username")
    val ftp_password = ftp_conf_pro.getProperty("ftp_password")
    val ftp_timeout = ftp_conf_pro.getProperty("ftp_timeout").toInt
    val ftp_encoding = ftp_conf_pro.getProperty("ftp_encoding")
    val ftp_workpath = ftp_conf_pro.getProperty("ftp_workpath")
    val ftp_ispassivemode =ftp_conf_pro.getProperty("ftp_ispassivemode")
    val ftp_clientpool_size_driver = ftp_conf_pro.getProperty("ftp_clientpool_size_driver").toInt


    val fconf = new FTPClientConfigure
    fconf.setEncoding(ftp_encoding)
    fconf.setHost(ftp_hostname)
    fconf.setPort(ftp_port)
    fconf.setClientTimeout(ftp_timeout)
    fconf.setUsername(ftp_username)
    fconf.setPassword(ftp_password)
    fconf.setWorkPath(ftp_workpath)
    fconf.setTransferFileType(FTP.BINARY_FILE_TYPE)
    fconf.setPassiveMode(ftp_ispassivemode)
    val ftpClientFactory = new FTPClientFactory(fconf)
    val ftpClientPool = new FTPClientPool(ftpClientFactory)
    if(isDriver){
      ftpClientPool.initPool(ftp_clientpool_size_driver)
    }
    ftpClientPool
  }

  def initPg():Unit={
    val pgdb_ip = appConfPro.getProperty("pgdb_ip")
    val pgdb_port = appConfPro.getProperty("pgdb_port").toInt
    val pgdb_user = appConfPro.getProperty("pgdb_user")
    val pgdb_passwd = appConfPro.getProperty("pgdb_passwd")
    val pgdb_db = appConfPro.getProperty("pgdb_db")
    //初始化pg数据库
    PgMergeLogicTb.init(pgdb_ip, pgdb_port, pgdb_user, pgdb_passwd, pgdb_db)
  }


}
