package com.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection._

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
      conn.setAutoCommit(false)
      preparedStatement = conn.prepareStatement(
        """insert into merge_data_log(datetime,city_date_hour,log)
          | values(to_timestamp(?, 'YYYY-MM-DD HH24:MI:SS.MS'), ?, ?)""".stripMargin,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)
//      true
//    } catch {
//      case ex: Exception =>
//        exceptionMsg = String.join("\n",
//          ex.getMessage, ex.getStackTrace.mkString("\n"), if (ex.getCause==null) "" else ex.getCause.toString)
//        false
//    }
  }

  def insertLog(city_date_hour: String, log: String): Boolean = {
    try {
      preparedStatement.setString(1, sm.format(new Date))
      preparedStatement.setString(2, city_date_hour)
      preparedStatement.setString(3, log)
      preparedStatement.addBatch()
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

  def execBatch: Boolean = {
    try {
      val cnt = preparedStatement.executeBatch()
      if (cnt.count(_=>true) > 0) {
        conn.commit()
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

  def end(): Unit = {
    if (preparedStatement!=null && !preparedStatement.isClosed)
      preparedStatement.close()
    if (conn != null && !conn.isClosed)
      conn.close()
  }

  def execBatch2(msgList: mutable.ListBuffer[(String, String)]): Boolean = {
    this.synchronized{
      msgList.foreach(p=>{
        preparedStatement.setString(1, p._1)
        preparedStatement.setString(2, null)
        preparedStatement.setString(3, p._2)
        preparedStatement.addBatch()
      })
      msgList.clear()
      execBatch
    }
  }

  def execBatch3(msgList: mutable.ListBuffer[(String, String, String)]): Boolean = {
    this.synchronized{
      msgList.foreach(p=>{
        preparedStatement.setString(1, p._1)
        preparedStatement.setString(2, p._2)
        preparedStatement.setString(3, p._3)
        preparedStatement.addBatch()
      })
      msgList.clear()
      execBatch
    }
  }

  def getExceptionMsg: String = exceptionMsg
}
