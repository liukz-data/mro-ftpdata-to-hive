package com.cmdi.mro_ftpdata_to_hive.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection._

/**
  * 将executor上Exeception日志写入merge_data_log
  */
object LogToPgDBTool {
  private var conn: Connection = _
  private var preparedStatement: PreparedStatement = _
  private var exceptionMsg: String = _
  private val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  classOf[org.postgresql.Driver]

  def init(pgdb_ip: String, pgdb_port: Int,
           pgdb_user: String, pgdb_passwd: String, pgdb_db: String): Unit = {
    val conn_str = s"jdbc:postgresql://$pgdb_ip:$pgdb_port/$pgdb_db?user=$pgdb_user&password=$pgdb_passwd"
//    try {
      conn = DriverManager.getConnection(conn_str)
      conn.setAutoCommit(true)
      preparedStatement = conn.prepareStatement(
        """insert into merge_data_log(datetime,city_date_hour,log)
          | values(to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS.MS'), ?, ?)""".stripMargin)
//      true
//    } catch {
//      case ex: Exception =>
//        exceptionMsg = String.join("\n",
//          ex.getMessage, ex.getStackTrace.mkString("\n"), if (ex.getCause==null) "" else ex.getCause.toString)
//        false
//    }
  }

  def insertMergeLog(city_date_hour: String, log: String): Unit ={
      this.synchronized{
        preparedStatement.setString(1, sm.format(new Date))
        preparedStatement.setString(2, city_date_hour)
        preparedStatement.setString(3, log)
        preparedStatement.execute()

      }
  }

  def end(): Unit = {
    if (preparedStatement!=null && !preparedStatement.isClosed)
      preparedStatement.close()
    if (conn != null && !conn.isClosed)
      conn.close()
  }

  def getExceptionMsg: String = exceptionMsg
}
