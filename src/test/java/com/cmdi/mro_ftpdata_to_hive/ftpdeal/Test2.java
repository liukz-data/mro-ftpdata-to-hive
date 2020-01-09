package com.cmdi.mro_ftpdata_to_hive.ftpdeal;

import com.cmdi.mro_ftpdata_to_hive.util.MroFieldTypeConvert;
import com.cmdi.mro_ftpdata_to_hive.util.MroFieldTypeConvertUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

public class Test2 {
    public static void main(String[] args) throws Exception {
        MroFieldTypeConvertUtil mroFieldTypeConvertUtil = new MroFieldTypeConvertUtil();
        mroFieldTypeConvertUtil.convertInit();
        System.out.println(mroFieldTypeConvertUtil.executeConvert("short","1237777779999999999"));
        /*Class<MroFieldTypeConvert> mroFieldTypeConvert = MroFieldTypeConvert.class;
        MroFieldTypeConvert mroFieldTypeConvertO = mroFieldTypeConvert.newInstance();
        Method[] methods = mroFieldTypeConvert.getDeclaredMethods();
        Map<String,Method> map = new LinkedHashMap<String,Method>();
        for(Method method:methods){
            String methodName = method.getName().toLowerCase();
            if(methodName.contains("int")){
                map.put("int",method);

            }else if(methodName.contains("short")){
                map.put("short",method);
            }else if(methodName.contains("byte")){
                map.put("byte",method);
            }else if(methodName.contains("decimal")){
                map.put("bigint",method);
            }
        }

        System.out.println(map.get("short").invoke(mroFieldTypeConvertO,"333"));
*/
    }
}
