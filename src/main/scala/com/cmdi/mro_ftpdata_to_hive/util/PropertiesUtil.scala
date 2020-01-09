package com.cmdi.mro_ftpdata_to_hive.util

import java.io.FileInputStream
import java.util.Properties

object PropertiesUtil {

  def getDiskProperties(proFilePath:String):Properties={

    val in = new FileInputStream(proFilePath)
    val pro = new Properties()
    pro.load(in)
    pro
  }
}
