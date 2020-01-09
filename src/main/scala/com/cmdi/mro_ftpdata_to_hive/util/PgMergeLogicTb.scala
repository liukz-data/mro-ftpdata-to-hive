package com.cmdi.mro_ftpdata_to_hive.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat


/**
  * pg库中插入和查询表mro_merge_callogic的一个工具类
  */
object PgMergeLogicTb {
  private var conn: Connection = _
  private var preparedStatementInsert: PreparedStatement = _
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
    preparedStatementInsert = conn.prepareStatement(
        """insert into mro_merge_callogic(taskid,city,logdate,hour,shellkind,status)
          | values(?,?,?,?,?, ?)""".stripMargin,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)
    preparedStatementSelect = conn.prepareStatement("select count(1) stop_hour_count from mro_merge_callogic where city=? and logdate=? and shellkind='callogic_hour' and status='stop'")

  }

  def insert(taskid:Int,city:String,logdate:String,hour:String,shellkind:String,status:String): Boolean = {

    try {
      this.synchronized{
        preparedStatementInsert.setInt(1, taskid)
        preparedStatementInsert.setString(2, city)
        preparedStatementInsert.setString(3, logdate)
        preparedStatementInsert.setString(4, hour)
        preparedStatementInsert.setString(5, shellkind)
        preparedStatementInsert.setString(6, status)
        preparedStatementInsert.execute()
      }

      true
    } catch {
      case ex: Exception =>
        exceptionMsg = String.join("\n",
          ex.getMessage + {
            if (ex.getCause == null) "" else "\n" + ex.getCause.toString
          }, ex.getStackTrace.mkString("\n    ")
        )
        false
    }
  }

  def selectStopHourCount(city:String,logDate:String):Int={
    preparedStatementSelect.setString(1,city)
    preparedStatementSelect.setString(2,logDate)
    val resultSet = preparedStatementSelect.executeQuery()
    var stop_hour_count=0
    if (resultSet.next()){
      stop_hour_count = resultSet.getInt("stop_hour_count")
    }
    stop_hour_count
  }

  def end(): Unit = {
    if (preparedStatementInsert!=null && !preparedStatementInsert.isClosed)
      preparedStatementInsert.close()
    if (preparedStatementSelect!=null && !preparedStatementSelect.isClosed)
      preparedStatementSelect.close()
    if (conn != null && !conn.isClosed)
      conn.close()
  }


  def getExceptionMsg: String = exceptionMsg
}
