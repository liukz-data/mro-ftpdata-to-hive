package com.cmdi.mro_ftpdata_to_hive.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import scala.collection.mutable


/**
  * pg库中插入和查询表mro_merge_callogic的一个工具类
  */
object PgEnbidCityTb {
  private var conn: Connection = _
  private var preparedStatementSelect: PreparedStatement = _
  private var exceptionMsg: String = _
  private val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  classOf[org.postgresql.Driver]

  def init(pgdb_ip: String, pgdb_port: Int,
           pgdb_user: String, pgdb_passwd: String, pgdb_db: String): Unit = {
    val conn_str = s"jdbc:postgresql://$pgdb_ip:$pgdb_port/$pgdb_db?user=$pgdb_user&password=$pgdb_passwd"
//    try {
      conn = DriverManager.getConnection(conn_str)
      conn.setAutoCommit(true)
    preparedStatementSelect = conn.prepareStatement("select enbid,cityname from mro_enbid_cityname_map_py")

  }



  def selectEnbidCityname():mutable.HashMap[Int,String]={
    val resultSet = preparedStatementSelect.executeQuery()
    val enbidCitynameMap = new mutable.HashMap[Int,String]()
    while (resultSet.next()){
      val enbid = resultSet.getInt("enbid")
      val cityname = resultSet.getString("cityname")
      enbidCitynameMap(enbid) = cityname
    }
    enbidCitynameMap
  }

  def end(): Unit = {
    if (preparedStatementSelect!=null && !preparedStatementSelect.isClosed)
      preparedStatementSelect.close()
    if (conn != null && !conn.isClosed)
      conn.close()
  }


  def getExceptionMsg: String = exceptionMsg
}
