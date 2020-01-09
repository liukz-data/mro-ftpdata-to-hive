package com.cmdi.mro_ftpdata_to_hive.ftpdeal;

public class Test1 {

    public static void main(String[] args) throws Exception {
        int i = 100000000;
    long timeStart1 = System.currentTimeMillis();

        long timeEnd1 = System.currentTimeMillis();
        System.out.println(timeEnd1-timeStart1);
       /* long timeStart = System.currentTimeMillis();
        MroFieldTypeConvertUtil.convertInit();
        for (int j = 0; j < i; j++) {
            MroFieldTypeConvertUtil.executeConvert("int","1");
            MroFieldTypeConvertUtil.executeConvert("short","1");
            MroFieldTypeConvertUtil.executeConvert("byte","1");
            MroFieldTypeConvertUtil.executeConvert("bigint","1");
        }
        long timeEnd = System.currentTimeMillis();
        System.out.println(timeEnd-timeStart);*/



    }
}
