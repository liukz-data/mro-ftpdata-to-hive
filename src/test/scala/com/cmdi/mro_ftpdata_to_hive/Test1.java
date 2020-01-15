package com.cmdi.mro_ftpdata_to_hive;


import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientUtil;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import test.TestM;
import test.TestOt;
import test.TestP;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Test1 {
    public static void main(String[] args) throws InterruptedException, ClassNotFoundException {

        try{
            FTPConnectionClosedException e  = new FTPConnectionClosedException();
            throw e;
        }catch (IOException e){
            System.out.println(e.toString());
        }

        // Class.forName("test.TestM");
        //System.out.println(TestM.age);
        // System.out.println(TestM.age);
        /*System.out.println(Integer.MAX_VALUE);
        System.out.println(Integer.MAX_VALUE>Long.valueOf("20181211080000"));*/
        /*LinkedBlockingQueue linkedBlockingQueue = new LinkedBlockingQueue<FTPClientUtil>();
        linkedBlockingQueue.take();
        ArrayBlockingQueue queue = new ArrayBlockingQueue<FTPClientUtil>(10);
        System.out.println(queue);*/
   /*     String matchStr = "201812111111";
        if(matchStr.matches("^20\\d{6}")){
            System.out.println(111);
        }else if(matchStr.matches("^20\\d{8}")){
            System.out.println(123);
        }else{
            System.out.println("输入错误");
            System.exit(1);
        }*/
    }
}
