package com.cmdi.mro_ftpdata_to_hive.ftpdeal;

import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientConfigure;
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientFactory;
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientPool;
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientUtil;
import com.cmdi.mro_ftpdata_to_hive.util.CompressedFileUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class TestFtppool {

    public static void main(String[] args) throws Exception {

     /*   FTPClient ftpclient = FtpConnect.getConnect("could001",5151,"ftp1","#Cmdi2019Ftp1");
        String[] a= ftpclient.listNames();
        for (String b:a){
            System.out.println(b);
        }
        ftpclient.disconnect();*/
        FTPClientConfigure fconf = new FTPClientConfigure();
        fconf.setEncoding("UTF-8");
        fconf.setHost("could001");
        fconf.setPort(5151);
        fconf.setClientTimeout(30);
        fconf.setUsername("ftp1");
        fconf.setPassword("#Cmdi2019Ftp1");
        fconf.setWorkPath("/20181211_1/104211/test1");
        fconf.setTransferFileType(FTPClientUtil.BINARY_FILE_TYPE);
        fconf.setPassiveMode("true");
        FTPClientFactory ftpClientFactory = new FTPClientFactory(fconf);
        FTPClientPool ftpClientPool = new FTPClientPool(ftpClientFactory);
        ftpClientPool.initPool(3);
        FTPClient ftpClient = ftpClientPool.borrowObject();
        if (!ftpClient.isConnected()) {
            System.out.println("ftpClient IS NOT CONNECT, SYSTEM EXIT!!");
            System.exit(-1);
        }

        FTPFile[] ftpFiles = ftpClient.listFiles();
        for(FTPFile ftpFile:ftpFiles){
            String fileName = String.join("/","/20181211_1/104211/test1",ftpFile.getName());
            long ftpSize = ftpFile.getSize();
            //System.out.println(ftpSize/(1024.0*1024));

            //System.out.println(ftpClient);
            //System.out.println("filename:"+fileName);
            InputStream in = ftpClient.retrieveFileStream(fileName);
            ByteBuffer buffer = ByteBuffer.allocate((int) ftpFile.getSize());
            byte[] cache = new byte[1024*1024];
            int len = 0;
            //System.out.println("start in:"+in);
            while((len = in.read(cache))!=-1){
                buffer.put(cache,0,len);
                //System.out.println("cache.length:"+len);
            }
            in.close();
            ftpClient.completePendingCommand();
            //System.out.println("end in:"+in);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer.array());
            InputStream gzipInputStream = CompressedFileUtil.getCopressedFileStream(byteArrayInputStream,ftpFile.getName());
            //GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            XMLStreamReader xmlStreamReader= XMLInputFactory.newInstance().createXMLStreamReader(gzipInputStream);
            while(xmlStreamReader.hasNext()){
                //System.out.println(xmlStreamReader.next());

                //xmlStreamReader.next();
               // System.out.println(xmlStreamReader.getProperty("smr"));
                int xmlType = xmlStreamReader.next();
                System.out.print(xmlType);
                if(xmlType == XMLStreamConstants.START_ELEMENT){
                    //xmlStreamReader.next();
                    String xmlName = xmlStreamReader.getName().toString();
                   // System.out.println(xmlName);
                   if("smr".equals(xmlName)){
                       System.out.println(xmlStreamReader.getElementText());
                   }
                }
               /* if(xmlStreamReader.next()== XMLStreamConstants.START_ELEMENT){
                    System.out.println(xmlStreamReader.getLocalName());
                    System.out.println(xmlStreamReader.getName().getNamespaceURI());
                    System.out.println(xmlStreamReader.getPrefix());
                    //System.out.println(xmlStreamReader.getElementText());
                }*/
            }

          /*  ZipInputStream zipInputStream = new ZipInputStream(byteArrayInputStream);
            ZipEntry zipEntry = null;

            while ((zipEntry = zipInputStream.getNextEntry()) != null){
                System.out.println(zipEntry.getName());
            }
            System.out.println(zipInputStream.getNextEntry());*/
        }

        ftpClientPool.close();

    }
}
