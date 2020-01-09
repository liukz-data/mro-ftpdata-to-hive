package com.cmdi.mro_ftpdata_to_hive.util;

import com.cmdi.mro_ftpdata_to_hive.bean.FTPFileInfo;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 发现FTP上符合匹配规则正则的gz或zip文件
 */
public class FTPFindCompressedFileUtil {

    private List<FTPFileInfo> ftpFilesList = new LinkedList<>();


    /**
     * 递归获得想要的文件
     * @param ftpClient ftp客户端
     * @param ftpPath 路径
     * @param regularStr 正则匹配规则
     * @return List<FTPFile>
     * @throws IOException
     */
    public List<FTPFileInfo> getAllCompressedFile(FTPClient ftpClient, String ftpPath, String regularStr) throws IOException {

        FTPFile[] ftpFiles = ftpClient.listFiles(ftpPath);
        for (FTPFile ftpFile : ftpFiles) {
            String ftpFileName = ftpFile.getName();
           // String ftpFileName = new String(ftpFile.getName().getBytes("iso-8859-1"),"UTF-8");
            String filePath = String.join("/", ftpPath, ftpFileName);
            // String filePath = new String(String.join("/", ftpPath, ftpFileName).getBytes("iso-8859-1"),"UTF-8");

            if (ftpFile.isFile()) {
                if (ftpFileName.matches(regularStr)) {
                    //处理文件名得到enbid和fileDateStrSub信息
                    String[] fileNameArr = ftpFileName.split("_");
                    //System.out.println(filePath);
                    int enbid = -123;
                    String fileNameTimeStr = fileNameArr[fileNameArr.length-1];
                    if(fileNameTimeStr.length() == 21){
                        enbid = Integer.parseInt(fileNameArr[fileNameArr.length-2]);
                    }else{
                        fileNameTimeStr = fileNameArr[fileNameArr.length-2];
                        enbid = Integer.parseInt(fileNameArr[fileNameArr.length-3]);
                    }
                    String fileDateStr =fileNameTimeStr.substring(0,8);
                    FTPFileInfo ftpFileInfo = new FTPFileInfo(ftpFileName,filePath,ftpPath,ftpFile.getSize(),ftpFile,enbid,fileDateStr,fileNameTimeStr.substring(8,10));
                    ftpFilesList.add(ftpFileInfo);
                } else {
                    //System.out.println("文件：" + filePath + "不符合匹配规则");
                }
            } else {
                getAllCompressedFile(ftpClient, filePath, regularStr);
            }
        }
        return ftpFilesList;
    }


    public List<FTPFileInfo> getFtpFilesList() {
        return ftpFilesList;
    }

    public void setFtpFilesList(List<FTPFileInfo> ftpFilesList) {
        this.ftpFilesList = ftpFilesList;
    }
}
