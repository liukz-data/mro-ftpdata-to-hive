package com.cmdi.mro_ftpdata_to_hive.bean;

import org.apache.commons.net.ftp.FTPFile;

import java.io.Serializable;

/**
 * 此类实际为FTPFile的扩展类，其内部封装了从ftp下载文件所需要的属性
 */
public class FTPFileInfo  implements Serializable {
    private String fileName;
    private String absolutePath;
    private String parentPath;
    private long fileSize;
    private FTPFile ftpFile;
    private int enbid;
    private String fileDate;
    private boolean existEnbid=false;
    private String fileHour;
    private String cityName;

    public FTPFileInfo(){}

    public FTPFileInfo(String fileName, String absolutePath, String parentPath, long fileSize, FTPFile ftpFile, int enbid, String fileDate,String fileHour) {
        this.fileName = fileName;
        this.absolutePath = absolutePath;
        this.parentPath = parentPath;
        this.fileSize = fileSize;
        this.ftpFile = ftpFile;
        this.enbid = enbid;
        this.fileDate = fileDate;
        this.fileHour = fileHour;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public void setAbsolutePath(String absolutePath) {
        this.absolutePath = absolutePath;
    }

    public String getParentPath() {
        return parentPath;
    }

    public void setParentPath(String parentPath) {
        this.parentPath = parentPath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public FTPFile getFtpFile() {
        return ftpFile;
    }

    public void setFtpFile(FTPFile ftpFile) {
        this.ftpFile = ftpFile;
    }

    public int getEnbid() {
        return enbid;
    }

    public void setEnbid(int enbid) {
        this.enbid = enbid;
    }

    public String getFileDate() {
        return fileDate;
    }

    public void setFileDate(String fileDate) {
        this.fileDate = fileDate;
    }

    public boolean isExistEnbid() {
        return existEnbid;
    }

    public void setExistEnbid(boolean existEnbid) {
        this.existEnbid = existEnbid;
    }

    public String getFileHour() {
        return fileHour;
    }

    public void setFileHour(String fileHour) {
        this.fileHour = fileHour;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    @Override
    public String toString() {
        return "FTPFileInfo{" +
                "fileName='" + fileName + '\'' +
                ", absolutePath='" + absolutePath + '\'' +
                ", parentPath='" + parentPath + '\'' +
                ", fileSize=" + fileSize +
                ", ftpFile=" + ftpFile +
                ", enbid=" + enbid +
                ", fileDate='" + fileDate + '\'' +
                ", existEnbid=" + existEnbid +
                ", fileHour='" + fileHour + '\'' +
                ", cityName='" + cityName + '\'' +
                '}';
    }
}
