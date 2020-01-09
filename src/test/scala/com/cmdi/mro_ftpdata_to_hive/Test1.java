package com.cmdi.mro_ftpdata_to_hive;


import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientUtil;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Test1 {
    public static void main(String[] args) throws InterruptedException {
        System.out.println(Integer.MAX_VALUE);
        System.out.println(Integer.MAX_VALUE>Long.valueOf("20181211080000"));
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
