package com.cmdi.mro_ftpdata_to_hive.ftp;

import org.apache.commons.net.ftp.FTPClient;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 继承FTPClient，作为程序的ftp客户端使用
 */
public class FTPClientUtil extends FTPClient  implements Serializable {
    private LinkedBlockingQueue arrayBlockingQueue;

    /**
     *
     * @param arrayBlockingQueue ftp连接池内的阻塞队列
     */
    public FTPClientUtil(LinkedBlockingQueue arrayBlockingQueue){
        this.arrayBlockingQueue=arrayBlockingQueue;
    }

    /**
     * 不进行真正的关闭，逻辑上的关闭
     */
    public void close(){
        arrayBlockingQueue.offer(this);
    }
}
