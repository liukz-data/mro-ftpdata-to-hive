package com.cmdi.mro_ftpdata_to_hive.util

import java.util

import com.cmdi.mro_ftpdata_to_hive.bean.FTPFileInfo
import com.cmdi.mro_ftpdata_to_hive.ftp.{FTPClientConfigure, FTPClientFactory, FTPClientPool}
import org.apache.commons.net.ftp.{FTP, FTPClient}

class Test2 {

  def main(args: Array[String]): Unit = {
    val fconf: FTPClientConfigure = new FTPClientConfigure
    fconf.setEncoding("UTF-8")
    fconf.setHost("could001")
    fconf.setPort(5151)
    fconf.setClientTimeout(30)
    fconf.setUsername("ftp1")
    fconf.setPassword("#Cmdi2019Ftp1")
    fconf.setWorkPath("/20181211_1/104211")
    fconf.setTransferFileType(FTP.BINARY_FILE_TYPE)
    fconf.setPassiveMode("true")
    val ftpClientFactory: FTPClientFactory = new FTPClientFactory(fconf)
    val ftpClientPool: FTPClientPool = new FTPClientPool( ftpClientFactory)
    ftpClientPool.initPool(3)
    val ftpClient: FTPClient = ftpClientPool.borrowObject
    if (!ftpClient.isConnected) {
      System.out.println("ftpClient IS NOT CONNECT, SYSTEM EXIT!!")
      System.exit(-1)
    }

    val ftpFiles: util.List[FTPFileInfo] = new FTPFindCompressedFileUtil().getAllCompressedFile(ftpClient, "/20181211_1/104211", ".*20181211.*")

    import scala.collection.JavaConversions._
    for (ftpFile1 <- ftpFiles) {
      System.out.println(ftpFile1.getFileName)
      System.out.println(ftpFile1.getAbsolutePath)
      System.out.println(ftpFile1.getParentPath)
      System.out.println(ftpFile1.getFileSize)
    }


    ftpClientPool.close()
  }
}
