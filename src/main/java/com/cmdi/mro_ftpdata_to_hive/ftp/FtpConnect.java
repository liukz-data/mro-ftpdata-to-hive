package com.cmdi.mro_ftpdata_to_hive.ftp;

import org.apache.commons.net.ftp.FTPClient;

public class FtpConnect {
    private static FTPClient fc;

    private FtpConnect() {
    }

    static {
        fc = new FTPClient();
    }

    public static FTPClient getConnect(String hostname, int port, String userName, String passwd) throws Exception {
        //ip，port
        //fc.connect("192.168.43.5",21);
        //username，passwd
        //fc.login("test1","test1");
        fc.connect(hostname, port);
        fc.login(userName, passwd);
        fc.enterLocalPassiveMode();
        return fc;
    }

    public static void disConnect() throws Exception {
        fc.disconnect();

    }
}
