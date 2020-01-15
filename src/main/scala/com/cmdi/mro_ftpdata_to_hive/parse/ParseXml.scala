package com.cmdi.mro_ftpdata_to_hive.parse


import java.io.{EOFException, InputStream}

import com.cmdi.mro_ftpdata_to_hive.bean.FTPFileInfo
import com.cmdi.mro_ftpdata_to_hive.ftp.FTPClientPool
import com.cmdi.mro_ftpdata_to_hive.util.{CompressedFileUtil, FileLoadToLocal, LogToPgDBTool, MroFieldTypeConvertUtil}
import com.ctc.wstx.exc.WstxIOException
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 用来解析xml文件的工具对象
  */
object ParseXml {

  val logger = Logger.getLogger("com.cmdi.mro_ftpdata_to_hive.parse.ParseXml")

  /**
    * 解析xml的逻辑
    *
    * @param ftpClientPool ftp 客户端连接池
    * @param ftpFileInfo   tp文件信息bean
    * @return ArrayBuffer[Array[Any]]
    **/
  def parseXml(ftpClientPool: FTPClientPool, ftpFileInfo: FTPFileInfo, parseJson: ParseJson, mroFieldTypeConvertUtil: MroFieldTypeConvertUtil, nameMoldToMap: Map[String, String]): ArrayBuffer[Array[Any]] = {
    val nameMold = parseJson.nameMold
    val nameArr = parseJson.nameArr.toArray
    val ncNameArr = parseJson.ncNameArr.toArray
    val ncDataStartIndex = parseJson.ncDataStartIndex
    val ncMoldArr = parseJson.ncMoldArr

    val ftpClient = ftpClientPool.borrowObject()
    val fileAllPath = ftpFileInfo.getAbsolutePath
    // Logger.getLogger("com.cmdi").warn("fileAllPath:"+fileAllPath)

    val ftpFileInputStream = ftpClient.retrieveFileStream(fileAllPath)
    val byteArrayInputStream = FileLoadToLocal.fileLoadToLocalAndGetInputStream(ftpFileInfo.getFileSize.toInt, ftpFileInputStream)
    ftpFileInputStream.close()
    ftpClient.completePendingCommand() //必须有这一步否则下一次遍历ftpFileInputStream会为null
    ftpClient.close()

    //存储一个映射关系：key为sc字段在nameArr中的索引，value为xml中字符串数组的索引
    val schemaIndexMapSc = new mutable.LinkedHashMap[Int, Int]()
    //存储一个映射关系：key为nc字段在ncNameArr中的索引，value为xml中字符串数组的索引
    val schemaIndexMapNc = new mutable.LinkedHashMap[Int, Int]()

    //存储mrokey到在objctArray索引的映射关系,smr为2时起作用
    val mroKeyStrArr = new ArrayBuffer[String]()
    //存储mrokey在objectArray索引的映射关系，smr为3时起作用
    val mroKeyStrMapSmr3 = new mutable.LinkedHashMap[String, mutable.HashSet[Int]]()
    //存储整个文件的符合条件的数据，对应hive中多行数据
    val objctArray = new ArrayBuffer[Array[Any]]()
    val objectStrSize = nameArr.size + 3
    //对应数据库中一行数据
    var objectStr: Array[Any] = null
    //变量存储一个mrokey，目的是为了smr2 smr3找到自身所对应的object
    var mroKeyStr2Key: String = null

    //smr3标签下，object属性中id字符串中对应的序号，目的是过滤出序号韦2和7的object
    var currSmr3Seq: String = null

    var enbId = ""
    //xml中该object的第几个v
    var ncVCount = 1
    //保证xml中一个objcet的有关sc的字段逻辑只运行一次
    var scVCount = 1
    //smr计数
    var smrCount = 0
    //追踪object索引位置
    var objIndex = -1

    var copressedFileStream:InputStream = null
    var xmlStreamReader: XMLStreamReader = null
    try {
      copressedFileStream = CompressedFileUtil.getCopressedFileStream(byteArrayInputStream, fileAllPath)
      xmlStreamReader = XMLInputFactory.newInstance.createXMLStreamReader(copressedFileStream)

      while (xmlStreamReader.hasNext) {
        var xmlType = xmlStreamReader.next()
        if (xmlType == XMLStreamConstants.START_ELEMENT) {
          val xmlName = xmlStreamReader.getName().toString()

          if ("eNB".equals(xmlName)) {
            enbId = xmlStreamReader.getAttributeValue(null, "id")
          } else if ("smr".equals(xmlName)) {
            smrCount = smrCount + 1
            //smr标签中存放着schema信息，有一个新的smr，就清理一次map
            schemaIndexMapSc.clear()
            schemaIndexMapNc.clear()
            val smrValue = xmlStreamReader.getElementText()
            var smrValueReplace = smrValue.replace("MR.", "")

            //当smr标签计数到第三个的时候，对其内部rip标签进行处理
            if (smrCount == 3 && smrValue.toLowerCase().contains("rip")) {
              smrValueReplace = "ltescrip2 ltescrip7"
            }
            val smrValueArr = smrValueReplace.split(" ")

            var i = 0
            //遍历smr中字符串数组，整个字符串数组为其标签下面数据的一个表头
            smrValueArr.foreach(smr => {
              val smrField = smr.toLowerCase()
              //根据字符串名称（相当于hive表中列名），找到该列在数组中所处的索引位置
              val nameIndexSc = nameArr.indexOf(smrField)
              val nameIndexNc = ncNameArr.indexOf(smrField)
              if (nameIndexSc != -1) {
                //以sc列名 在nameArr索引为key，字符串数组中所在索引位置为value放入schemaIndexMapSc
                schemaIndexMapSc.put(nameIndexSc, i)
              } else if (nameIndexNc != -1) {
                //以nc列名 在nameArr索引为key，在字符串数组中所在索引位置为value放入schemaIndexMapNc
                schemaIndexMapNc.put(nameIndexNc, i)
              }
              i = i + 1
            })
            /*   schemaIndexMapSc.foreach(index => {
                 println(smrValueArr(index._2))
               })
               schemaIndexMapNc.foreach(index => {
                 println(smrValueArr(index._2))
               })*/

          } else {
            //若SMR中列名有符合条件的
            if (schemaIndexMapSc.nonEmpty || schemaIndexMapNc.nonEmpty) {
              if ("object".equals(xmlName)) {
                if (objectStr != null) {
                  objectStr = null
                }

                //重新为ncVCount，scVCount赋初始值
                ncVCount = 1
                scVCount = 1
                val timeStamp = xmlStreamReader.getAttributeValue(null, "TimeStamp")
                val eci = xmlStreamReader.getAttributeValue(null, "id")
                if (smrCount != 3) {
                  val mmeCode = xmlStreamReader.getAttributeValue(null, "MmeCode")
                  val mmeGroupId = xmlStreamReader.getAttributeValue(null, "MmeGroupId")
                  val mmeUeS1apId = xmlStreamReader.getAttributeValue(null, "MmeUeS1apId")
                  val mroKeyStr1 = mmeCode + mmeGroupId + mmeUeS1apId
                  val mroKeyStr2 = timeStamp + eci
                  val mroKeyStr = mroKeyStr1 + mroKeyStr2
                  //第一个smr逻辑
                  if (smrCount == 1) {
                    if (!eci.contains(":")) {
                      val cgi = enbId + "-" + (eci.toInt - enbId.toInt * 256).toString
                      val enbidConvert = mroFieldTypeConvertUtil.executeConvert(nameMoldToMap("enbid"), enbId)
                      val eciConvert = mroFieldTypeConvertUtil.executeConvert(nameMoldToMap("eci"), eci)
                      val cgiConvert = cgi
                      val mmeues1apidConvert = mroFieldTypeConvertUtil.executeConvert(nameMoldToMap("mmeues1apid"), mmeUeS1apId)
                      val mmegroupidConvert = mroFieldTypeConvertUtil.executeConvert(nameMoldToMap("mmegroupid"), mmeGroupId)
                      val mmecodeConvert = mroFieldTypeConvertUtil.executeConvert(nameMoldToMap("mmecode"), mmeCode)
                      val timestampConvert = timeStamp
                      objectStr = new Array[Any](objectStrSize)
                      //将hive表中city、logdate、hour放入数组
                      objectStr(objectStrSize - 3) = ftpFileInfo.getCityName
                      objectStr(objectStrSize - 2) = ftpFileInfo.getFileDate
                      objectStr(objectStrSize - 1) = ftpFileInfo.getFileHour
                      objctArray += objectStr
                      objectStr(nameArr.indexOf("enbid")) = enbidConvert
                      objectStr(nameArr.indexOf("eci")) = eciConvert
                      objectStr(nameArr.indexOf("cgi")) = cgiConvert
                      objectStr(nameArr.indexOf("mmeues1apid")) = mmeues1apidConvert
                      objectStr(nameArr.indexOf("mmegroupid")) = mmegroupidConvert
                      objectStr(nameArr.indexOf("mmecode")) = mmecodeConvert
                      objectStr(nameArr.indexOf("timestamp")) = timestampConvert
                      mroKeyStrArr += mroKeyStr
                      mroKeyStr2Key = mroKeyStr2
                      objIndex = objIndex + 1
                    }
                    //第二个smr逻辑
                  } else if (smrCount == 2) {
                    val mroObjectIndex = mroKeyStrArr.indexOf(mroKeyStr)
                    //若有此mrokey对应object，则拿出objctArray数组中object对象
                    if (mroObjectIndex != -1) {
                      objectStr = objctArray(mroObjectIndex)
                    }
                  }
                } else {
                  if (eci.contains(":")) {
                    val eciEarfcnRip = eci.split(":")
                    val mroKeyStr2 = timeStamp + eciEarfcnRip(0) + eciEarfcnRip(1)
                    mroKeyStr2Key = mroKeyStr2
                    currSmr3Seq = eciEarfcnRip(2)
                  }
                }
              } else if ("v".equals(xmlName)) {
                val valueStr = xmlStreamReader.getElementText
                if (smrCount != 3) {
                  val valueFields = valueStr.split(" ")
                  //对于sc数据只拿一次
                  if (scVCount == 1) {
                    schemaIndexMapSc.foreach(index => {
                      val valueFieldIndex = index._2
                      val valueFieldName = nameArr(index._1)
                      val scField = valueFields(valueFieldIndex)
                      //拼接出smr为第三个标签时的mrokey
                      if ("ltescearfcn".equals(valueFieldName)) {
                        mroKeyStr2Key = mroKeyStr2Key + scField
                        val currIndexSetOption = mroKeyStrMapSmr3.get(mroKeyStr2Key)
                        currIndexSetOption match {
                          case Some(currIndexSet) =>
                            currIndexSet.add(objIndex)
                          case None =>
                            val currIndexSet = new mutable.HashSet[Int]()
                            currIndexSet.add(objIndex)
                            mroKeyStrMapSmr3.put(mroKeyStr2Key, currIndexSet)
                        }

                      }
                      //将sc里的数据放入objectStr中对应位置
                      if (!"NIL".equals(scField)) {
                        /*Logger.getLogger("org").warn("nvalueFieldName:"+valueFieldName+" \nscField:"+scField)
                        Logger.getLogger("org").warn("nameMold:"+nameMold.toBuffer)
  */
                        // Logger.getLogger("org").warn("nameMold(valueFieldName):"+nameMold(valueFieldName))
                        val valueField = nameMoldToMap(valueFieldName)
                        //Logger.getLogger("org").warn("nameMold(valueFieldName):"+valueField)
                        objectStr(index._1) = mroFieldTypeConvertUtil.executeConvert(valueField, scField)

                      }
                    })
                    scVCount = scVCount + 1
                  }

                  //控制每个object中拿取nc的条数
                  if (ncVCount <= parseJson.nc_num) {
                    schemaIndexMapNc.foreach(index => {
                      val ncField = valueFields(index._2)
                      if (!"NIL".equals(ncField)) {
                        //计算出此nc字段数据在objectStr中位置，并放入objectStr
                        objectStr(ncDataStartIndex + (ncVCount - 1) * ncNameArr.size + index._1) = mroFieldTypeConvertUtil.executeConvert(ncMoldArr(index._1), valueFields(index._2))
                      }
                    })
                  }
                  ncVCount = ncVCount + 1
                } else {
                  //找到第三个smr标签下对应的全部object，将对应数据放入object
                  val curHashSet = mroKeyStrMapSmr3.get(mroKeyStr2Key)
                  curHashSet match {
                    case Some(curHashSet) =>
                      curHashSet.foreach(index => {
                        val objectData = objctArray(index)
                        currSmr3Seq match {
                          case "2" =>
                            val ltescrip2Index = nameArr.indexOf("ltescrip2")
                            objectData(ltescrip2Index) = mroFieldTypeConvertUtil.executeConvert(nameMoldToMap("ltescrip2"), valueStr)
                          case "7" =>
                            objectData(nameArr.indexOf("ltescrip7")) = mroFieldTypeConvertUtil.executeConvert(nameMoldToMap("ltescrip7"), valueStr)
                          case _ =>

                        }
                      })
                    case None =>
                    //println("mroKeyStr2Key:"+mroKeyStr2Key)
                  }
                }

              }
            }
          }

        }
      }
    } catch {
      case ex: WstxIOException => {
        val exceptionMsg = String.join("\n    ",
          ex.getClass.getName+": "+ ex.getMessage, ex.getStackTrace.mkString("\n    "), {
            if (ex.getCause == null) "" else "\nCaused by: "+ex.getCause.toString+"\n    "+ex.getCause.getStackTrace.mkString("\n    ")
          },
          "Error File:" + fileAllPath)
        logger.error(exceptionMsg)
        //错误日志记录到pg
        LogToPgDBTool.insertMergeLog(String.join("_",ftpFileInfo.getCityName,ftpFileInfo.getFileDate,ftpFileInfo.getFileHour),exceptionMsg)
        return new ArrayBuffer[Array[Any]]()
      }
      case ex: EOFException => {
        val exceptionMsg = String.join("\n    ",
          ex.getClass.getName+": "+ ex.getMessage, ex.getStackTrace.mkString("\n    "), {
            if (ex.getCause == null) "" else "\nCaused by: "+ex.getCause.toString+"\n    "+ex.getCause.getStackTrace.mkString("\n    ")
          },
          "Error File:" + fileAllPath)
        logger.error(exceptionMsg)
        //错误日志记录到pg
        LogToPgDBTool.insertMergeLog(String.join("_",ftpFileInfo.getCityName,ftpFileInfo.getFileDate,ftpFileInfo.getFileHour),exceptionMsg)
        return new ArrayBuffer[Array[Any]]()
      }
    }finally {
      if(xmlStreamReader != null)      xmlStreamReader.close()
      if(copressedFileStream != null)  copressedFileStream.close()
    }



    //ftpFileInputStream.close()

    /* ftpClient.completePendingCommand //必须有这一步否则下一次遍历ftpFileInputStream会为null
     ftpClient.close()*/

    objctArray
  }
}
