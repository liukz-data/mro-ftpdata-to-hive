package com.cmdi.mro_ftpdata_to_hive.util;


import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 执行类型转换的工具类
 */
public class MroFieldTypeConvert implements Serializable {
    public Byte toByte(String toBe){
        if(new BigDecimal(toBe).abs().compareTo(new BigDecimal(Byte.MAX_VALUE))==1){
            return  (byte) -123;
        }
        return  Byte.parseByte(toBe);
    }

    public Short toShort(String toBe){
        if(new BigDecimal(toBe).abs().compareTo(new BigDecimal(Short.MAX_VALUE))==1){
            return  (short) -123;
        }
        return Short.parseShort(toBe);
    }


    public  Integer toInt(String toBe){
        if(new BigDecimal(toBe).abs().compareTo(new BigDecimal(Integer.MAX_VALUE)) == 1){
            return  -123;
        }
        return Integer.parseInt(toBe);
    }

    public Long toBigInt(String toBe){
        return Long.valueOf(toBe);
    }
}
