package com.cmdi.mro_ftpdata_to_hive.ftpdeal;

import com.cmdi.mro_ftpdata_to_hive.bean.FTPFileInfo;

import static org.jboss.netty.util.CharsetUtil.ISO_8859_1;

public class TestObj1 {

    public static void main(String[] args) {
        FTPFileInfo ftpFileInfo = new FTPFileInfo();
        m1(ftpFileInfo);
        System.out.println(ftpFileInfo.getAbsolutePath());
    }
    public static void m1(FTPFileInfo ftpFileInfo){
        ftpFileInfo.setAbsolutePath("222");
    }
}
