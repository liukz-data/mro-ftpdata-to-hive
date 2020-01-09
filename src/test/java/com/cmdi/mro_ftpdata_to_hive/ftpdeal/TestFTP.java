package com.cmdi.mro_ftpdata_to_hive.ftpdeal;

import com.cmdi.mro_ftpdata_to_hive.bean.FTPFileInfo;
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientConfigure;
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientFactory;
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientPool;
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientUtil;
import com.cmdi.mro_ftpdata_to_hive.util.FTPFindCompressedFileUtil;
import org.apache.commons.net.ftp.FTPClient;

import java.util.List;

public class TestFTP {

    public static void main(String[] args) throws Exception {

     /*   FTPClient ftpclient = FtpConnect.getConnect("could001",5151,"ftp1","#Cmdi2019Ftp1");
        String[] a= ftpclient.listNames();
        for (String b:a){
            System.out.println(b);
        }
        ftpclient.disconnect();*/
        FTPClientConfigure fconf = new FTPClientConfigure();
        fconf.setEncoding("UTF-8");
        fconf.setHost("could001");
        fconf.setPort(5151);
        fconf.setClientTimeout(30);
        fconf.setUsername("ftp1");
        fconf.setPassword("#Cmdi2019Ftp1");
        fconf.setWorkPath("/20181211_1/104211");
        fconf.setTransferFileType(FTPClientUtil.BINARY_FILE_TYPE);
        fconf.setPassiveMode("true");
        FTPClientFactory ftpClientFactory = new FTPClientFactory(fconf);
        FTPClientPool ftpClientPool = new FTPClientPool(ftpClientFactory);
        ftpClientPool.initPool(3);
        FTPClient ftpClient = ftpClientPool.borrowObject();
        if (!ftpClient.isConnected()) {
            System.out.println("ftpClient IS NOT CONNECT, SYSTEM EXIT!!");
            System.exit(-1);
        }

       List<FTPFileInfo> ftpFiles = new FTPFindCompressedFileUtil().getAllCompressedFile(ftpClient,"/20181211_1/104211",".*20181211.*");

        for(FTPFileInfo ftpFile1:ftpFiles){
            System.out.println(ftpFile1.getFileName());
            System.out.println(ftpFile1.getAbsolutePath());
            System.out.println(ftpFile1.getParentPath());
            System.out.println(ftpFile1.getFileSize());
        }


        ftpClientPool.close();

    }
}
