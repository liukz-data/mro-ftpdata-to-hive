package com.cmdi.mro_ftpdata_to_hive.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * FTPClient工厂类，通过FTPClient工厂提供FTPClient实例的创建和销毁
 * @author heaven
 */
public class FTPClientFactory implements Serializable {
    private static Logger logger = LoggerFactory.getLogger("FtpClientFactory");
    private FTPClientConfigure config;

    //给工厂传入一个参数对象，方便配置FTPClient的相关参数
    public FTPClientFactory(FTPClientConfigure config) {
        this.config = config;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool.PoolableObjectFactory#makeObject()
     */
    public FTPClientUtil makeObject(LinkedBlockingQueue<FTPClientUtil> arrayBlockingQueue) throws Exception {

        FTPClientUtil ftpClient = new FTPClientUtil(arrayBlockingQueue);
        ftpClient.setConnectTimeout(config.getClientTimeout());
            ftpClient.connect(config.getHost(), config.getPort());
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                logger.warn("FTPServer refused connection");
                throw new IOException("FTPServer refused connection");
            }


            boolean result = ftpClient.login(config.getUsername(), config.getPassword());
            if (!result) {
                throw new Exception("ftpClient登陆失败! userName:" + config.getUsername() + " ; password:" + config.getPassword());
            }
            //ftpClient.setFileType(config.getTransferFileType());
            //ftpClient.sendCommand("OPTS UTF8", "ON");
            ftpClient.setBufferSize(1024*1024*100);
            if(!ftpClient.changeWorkingDirectory(config.getWorkPath())) throw new FileNotFoundException(config.getWorkPath());
            ftpClient.setFileType(config.getTransferFileType());
            ftpClient.setControlEncoding(config.getEncoding());
            if (config.getPassiveMode().equals("true")) {
                ftpClient.enterLocalPassiveMode();
            }

        return ftpClient;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool.PoolableObjectFactory#destroyObject(java.lang.Object)
     */
    public void destroyObject(Object o) throws Exception {
        FTPClient ftpClient = (FTPClient) o;
        try {
            if (ftpClient != null && ftpClient.isConnected()) {
                ftpClient.logout();
            }
        }  finally {
            // 注意,一定要在finally代码中断开连接，否则会导致占用ftp连接情况
            try {
                ftpClient.disconnect();
            } catch (IOException io) {
                io.printStackTrace();
            }
        }
    }
    public void realDestroyObject(Object o) throws Exception {
        FTPClient ftpClient = (FTPClient) o;
        try {
            if (ftpClient != null && ftpClient.isConnected()) {
                ftpClient.logout();
            }
        }  finally {
            // 注意,一定要在finally代码中断开连接，否则会导致占用ftp连接情况
            try {
                ftpClient.disconnect();
            } catch (IOException io) {
                io.printStackTrace();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool.PoolableObjectFactory#validateObject(java.lang.Object)
     */
    public boolean validateObject(Object o) throws IOException {
        FTPClient ftpClient = (FTPClient) o;/*
        try {*/
            return ftpClient.sendNoOp();/*
        } catch (IOException e) {
            throw new RuntimeException("Failed to validate client: " + e, e);
        }*/
    }

    public void activateObject(Object o) throws Exception {
    }

    public void passivateObject(Object o) throws Exception {

    }
}
