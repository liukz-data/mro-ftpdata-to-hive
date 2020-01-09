package com.cmdi.mro_ftpdata_to_hive.parse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * 测试会用到此对象
  */
object AccessHdfsUtil {

  def test1(hadoopConf:Configuration):Unit={
   val fs = FileSystem.get(hadoopConf)
    val fsIn = fs.exists(new Path("/mro"))
  }
}
