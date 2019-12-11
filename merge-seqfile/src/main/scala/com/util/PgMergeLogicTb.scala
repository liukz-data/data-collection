package com.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection._

object PgMergeLogicTb {
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
    //insert into mro_merge_callogic(taskid,city,logdate,hour,shellkind,status) values(2,'test','test','test,'merge','start')
      preparedStatement = conn.prepareStatement(
        """insert into mro_merge_callogic(taskid,city,logdate,hour,shellkind,status)
          | values(?,?,?,?,?, ?)""".stripMargin,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)
//      true
//    } catch {
//      case ex: Exception =>
//        exceptionMsg = String.join("\n",
//          ex.getMessage, ex.getStackTrace.mkString("\n"), if (ex.getCause==null) "" else ex.getCause.toString)
//        false
//    }
  }

  def insert(taskid:Int,city:String,logdate:String,hour:String,shellkind:String,status:String): Boolean = {
    try {
      preparedStatement.setInt(1, taskid)
      preparedStatement.setString(2, city)
      preparedStatement.setString(3, logdate)
      preparedStatement.setString(4, hour)
      preparedStatement.setString(5, shellkind)
      preparedStatement.setString(6, status)
      preparedStatement.execute()
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


  def getExceptionMsg: String = exceptionMsg
}
