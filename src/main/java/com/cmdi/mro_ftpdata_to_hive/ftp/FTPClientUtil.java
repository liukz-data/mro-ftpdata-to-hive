package com.cmdi.mro_ftpdata_to_hive.ftp;

import org.apache.commons.net.ftp.FTPClient;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class FTPClientUtil extends FTPClient  implements Serializable {
    private LinkedBlockingQueue arrayBlockingQueue;
    public FTPClientUtil(LinkedBlockingQueue arrayBlockingQueue){
        this.arrayBlockingQueue=arrayBlockingQueue;
    }

    public void close(){
        arrayBlockingQueue.offer(this);
    }
}
