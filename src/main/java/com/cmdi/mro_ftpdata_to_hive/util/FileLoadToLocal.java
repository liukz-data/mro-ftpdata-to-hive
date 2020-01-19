package com.cmdi.mro_ftpdata_to_hive.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 将ftp上文件下载到本地内存
 */
public class FileLoadToLocal {
    public static ByteArrayInputStream fileLoadToLocalAndGetInputStream(int ftpFileSize,InputStream in ) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ftpFileSize);
        byte[] cache = new byte[512];
        int len = 0;
        while((len = in.read(cache))!=-1){
            buffer.put(cache,0,len);
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer.array());
        return byteArrayInputStream;
    }
}
