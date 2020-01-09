package com.cmdi.mro_ftpdata_to_hive.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;


/**
 * zip和gz文件解压缩工具
 */
public class CompressedFileUtil {

    /**
     * 得到解压流 InputStream
     * @param in 输入的流
     * @param fileName 文件全名，如aaa.gz，aaa.zip
     * @return 返回GZIPInputStream 或者 ZipInputStream，否则返回null
     * @throws IOException
     */
    public static InputStream getCopressedFileStream(InputStream in,String fileName) throws IOException {
        String fileType = getCompressFileFileType(fileName);
        if("gz".equals(fileType)){
            GZIPInputStream gzipInputStream = new GZIPInputStream(in);
            return  gzipInputStream;
        }else if("zip".equals(fileType)){
            ZipInputStream zipInputStream = new ZipInputStream(in);
            zipInputStream.getNextEntry(); //仅一个zip压缩包有一个文件才能使用
            return zipInputStream;
        }else{
            throw new IOException("com.cmdi.mro_ftpdata_to_hive.util.CompressedFileUtil.getCopressedFileStream(InputStream in,String fileName) EXCEPTION: CompressedFileUtil 仅支持gz zip格式文件操作");
        }
    }


    /**
     * 获得压缩文件种类此处支持gz、zip
     * @param fileName 文件全称
     * @return
     */
    private static String getCompressFileFileType(String fileName){
        if(fileName.matches(".*\\.gz")){
            return  "gz";
        }else if(fileName.matches(".*\\.zip")){
            return "zip";
        }
        return "noknow";
    }


}
