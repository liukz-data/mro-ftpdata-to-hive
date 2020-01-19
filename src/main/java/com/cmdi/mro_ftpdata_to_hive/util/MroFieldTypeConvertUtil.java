package com.cmdi.mro_ftpdata_to_hive.util;

import java.io.Serializable;

/**
 * 此类为将字符串字段类型转换为byte，short，int，bigdecimal类型的工具类
 */
public class MroFieldTypeConvertUtil implements Serializable {

    private MroFieldTypeConvert mroConvert = new MroFieldTypeConvert();
    //key为数据类型，value为Method对象，
   // private Map<String, Method> stringMethodMap = new HashMap<String, Method>();
  //  private MroFieldTypeConvert mroFieldTypeConvertObj = null;

    //转换数据类型所需要的初始化，构造了stringMethodMap中<key，value>对应关系
  /*  public void convertInit() throws Exception {
        Class<MroFieldTypeConvert> mroFieldTypeConvert = MroFieldTypeConvert.class;
        mroFieldTypeConvertObj = mroFieldTypeConvert.newInstance();
        //利用java反射得到method对象数组
        Method[] methods = mroFieldTypeConvert.getDeclaredMethods();

        for (Method method : methods) {
            String methodName = method.getName().toLowerCase();
            if (methodName.contains("bigint")) {
                stringMethodMap.put("bigint", method);
            } else if (methodName.contains("int")) {
                stringMethodMap.put("int", method);
            } else if (methodName.contains("short")) {
                stringMethodMap.put("short", method);
            } else if (methodName.contains("byte")) {
                stringMethodMap.put("byte", method);
            } else {
                throw new Exception("com.cmdi.mro_ftpdata_to_hive.util.MroFieldTypeConvert 不支持这个类型的数据转换");
            }
        }

    }*/

    /**
     * 执行字符串转换
     * @param fieldType 字段类型，int，byte，short，bigint
     * @param fieldValue 字段的值
     * @return obj 转换后的值
     * @throws Exception 字段类型非法时抛出此异常
     */
    public Object executeConvert(String fieldType, String fieldValue) throws Exception {
        Object obj = null;
        if (fieldType.contains("bigint")) {
            obj = mroConvert.toBigInt(fieldValue);
        } else if (fieldType.contains("int")) {
            obj = mroConvert.toInt(fieldValue);
        } else if (fieldType.contains("short")) {
            obj = mroConvert.toShort(fieldValue);
        } else if (fieldType.contains("byte")) {
            obj = mroConvert.toByte(fieldValue);
        } else {
            throw new Exception("com.cmdi.mro_ftpdata_to_hive.util.MroFieldTypeConvert 不支持这个类型的数据转换");
        }
        return obj;
        /*String fieldTypeConvert = fieldType.toLowerCase();
        Method fieldMethod = stringMethodMap.get(fieldTypeConvert);
        //若转换的数据类型为非法数据类型，则抛出异常
        if (fieldMethod == null) throw new IllegalArgumentException();
        return fieldMethod.invoke(mroFieldTypeConvertObj,fieldValue);*/
    }
}
