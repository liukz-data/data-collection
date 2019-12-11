package com.table

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.mro.schema.FieldsMask
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
//import org.apache.hadoop.security.UserGroupInformation
//import java.io.File
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object MroTable {
  private val prop: Properties = new Properties()
  private var hadoopConf: Configuration = _
  var fs: FileSystem = null

  private val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    executeMerge(args)
  }
  private def getContent(path: String): String = {
    var fs: FileSystem = null

    try {
      fs = FileSystem.get(URI.create(path), this.hadoopConf)
      val is = fs.open(new Path(path))
      val baos = new ByteArrayOutputStream(4096)
      IOUtils.copyBytes(is, baos, this.hadoopConf)
      baos.toString
    } catch {
      case ex: Exception =>
        System.err.println(sm.format(new Date()) + s" ERROR MroTable: 读取文件 $path 时发生错误")
        ex.printStackTrace()
        System.exit(1)
        null
    } /*finally {
      //此地方相当于关闭了hdfs文件系统io，会造成kerberos认证异常，此处不能关闭io
      //原因是FileSystem的get方法会从缓存中拿出io，公用一个io
      IOUtils.closeStream(fs)
    }*/
  }

  private[table] def executeMerge(args: Array[String], hadoopConfMap: Map[String, String] = null): Unit = {

    var confFile: String = null
    var sparkLogLevel: String = "ERROR"

    if (args.length<1){
      System.err.println("使用方法: com.table.MroTable <配置文件路径> [<Spark日志级别，默认为error>]")
      System.exit(1)
    } else {
      confFile = args(0)
      if (args.length == 2)
        sparkLogLevel = args(1)
    }

    Logger.getLogger("org").setLevel(Level.toLevel(sparkLogLevel.toUpperCase, Level.ERROR))
    Logger.getLogger("hive").setLevel(Level.toLevel(sparkLogLevel.toUpperCase, Level.ERROR))

    // 0. 获取 Hadoop 配置
    println(sm.format(new Date()) + " INFO MroTable: 获取 Hadoop Configuration 配置内容")
    hadoopConf = new Configuration()
    if (hadoopConfMap != null) {
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      println("测试用")
      hadoopConfMap.foreach(item => {
        if (item._1.startsWith("spark.hadoop.")) {
          hadoopConf.set(item._1.substring(13), item._2)
        }
      })
    }

    // 1. 读取 conf/mromerge-conf.properties 里的内容
    println(sm.format(new Date()) + s" INFO MroTable: 读取配置文件 $confFile 里的内容")

    try {
      val content = getContent(confFile)
      val bais = new ByteArrayInputStream(content.getBytes)
      prop.load(bais)
    } catch {
      case ioe: IOException =>
        System.err.println(sm.format(new Date()) +
          s" ERROR MroTable: 读取配置文件 $confFile 的内容时遇到错误，程序异常退出！！！")
        ioe.printStackTrace()
        System.exit(1)
    }

    // 获取相关配置信息
    val mroobj_json = prop.getProperty("mroobj_json")
    val db_name = prop.getProperty("dbname", "mro")
    val tb_name = prop.getProperty("tb_name", "mro_dt_rec")
    val tb_compress = prop.getProperty("tb_compress","snappy")
    val tb_location = prop.getProperty("tb_location", "/mro/mro_dt_rec")

    // 2. 读取并解析 日志字段配置信息的json文件 的内容，生成 FieldsMask.allFieldsMask
    // 获取 日志字段json文件位置，并通过System.setProperty方法 传入 FieldsMask
    println(sm.format(new Date()) + " INFO MroTable: 读取日志字段配置JSON文件的内容")

    val jsonContent: String = getContent(mroobj_json)
    System.setProperty("SYS_MROOBJ_JSON", jsonContent)
    // 先判断 日志字段配置信息的json文件 文件是否能被正确解析，不能则退出程序
    try {
      FieldsMask.allFieldsMask() == null
    } catch {
      case ex: Exception =>
        System.err.println(sm.format(new Date()) +
          " ERROR MroTable: 日志字段配置信息json文件的内容没有被正确解析，MRO表创建程序退出！")
        ex.printStackTrace()
        System.exit(1)
    }
//    if (FieldsMask.allFieldsMask() == null) {
//      System.err.println(sm.format(new Date()) +
//        " ERROR MroTable: 日志字段配置信息json文件的内容没有被正确解析，MRO表创建程序退出！")
//      System.exit(1)
//    }

    // 获取拼接好的建表语句
    val creTblSql = getCreTblSql(s"$db_name.$tb_name", tb_compress, tb_location)

    // 构建SparkSQL的运行还境
    val sparkConf = new SparkConf().setAppName("MroTable")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.exec.orc.compression.strategy", "COMPRESSION")
      .set("hive.exec.orc.default.compress",
        prop.getProperty("tb_compress","SNAPPY").toUpperCase )

    if (hadoopConfMap != null){
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item=>{
        sparkConf.set(item._1, item._2)
      })
      // 本地测试用，设置 Master，默认为 local[2]
      sparkConf.setMaster(hadoopConfMap.getOrElse("spark_master", "local[2]"))
    }

    // 3. 如果DB不存在则先创建DB，如果MRO表存在则Drop掉原表，再生成新表
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

//      val hive_user = prop.getProperty("hive_user")
//      val hive_keytab = "C:\\Program Files\\MIT\\Kerberos\\mrdas-WY.keytab"
//      val hadoopConf1 = new Configuration()
//      hadoopConf1.addResource(
//        new File(".\\conf\\core-site.xml")
//          .toURI.toURL)
//      hadoopConf1.addResource(
//        new File(".\\conf\\hdfs-site.xml")
//          .toURI.toURL)
//      try {
//        UserGroupInformation.setConfiguration(hadoopConf1)
//        UserGroupInformation.loginUserFromKeytab(hive_user, hive_keytab)
//      } catch {
//        case exception: Exception =>
//          exception.printStackTrace()
//          System.exit(1)
//      }

    // 创建 MRO 数据库
    println(sm.format(new Date()) + s" INFO MroTable: 创建数据采集及共享项目Hive数据库 $db_name")
    var result: DataFrame = null
    try {
      result = spark.sql(
        s"create database if not exists $db_name comment '数据采集及共享项目Hive数据库'")
    } catch {
      case ex: Exception =>
        System.err.println(sm.format(new Date()) +
          s" ERROR MroTable: 创建数据库${db_name}时发生错误，程序退出")
        ex.printStackTrace()
        System.exit(1)
    }

    println(sm.format(new Date()) + s" INFO MroTable: 创建数据采集及共享项目MRO表 $tb_name")
    try {
      result = spark.sql(s"drop table if exists $db_name.$tb_name")
    } catch {
      case ex: Exception =>
        System.err.println(sm.format(new Date()) +
          s" ERROR MroTable: 删除表${tb_name}时发生错误，程序退出")
        ex.printStackTrace()
        System.exit(1)
    }
    try {
      result = spark.sql(creTblSql)
    } catch {
      case ex: Exception =>
        System.err.println(sm.format(new Date()) +
          s" ERROR MroTable: 创建表${tb_name}时发生错误，程序退出")
        ex.printStackTrace()
        System.exit(1)
    }
    spark.stop()
    IOUtils.closeStream(fs)
  }

  private def getCreTblSql(tb_name:String, tb_compress: String, tb_location: String): String={
    val sb: StringBuilder = new StringBuilder

    // 拼接 create table子句
    sb.append(s"create external table if not exists ${tb_name}(")

    for( i <- FieldsMask.allFields.indices ){
      if (FieldsMask.allFieldsMask().getOrElse(FieldsMask.allFields(i), (false,null))._1){
        sb.append(FieldsMask.allFields(i))
        FieldsMask.allFieldsMask().getOrElse(FieldsMask.allFields(i), (false,null))._2.toUpperCase match {
          case "BYTE" => sb.append(" TINYINT")
          case "SHORT" => sb.append(" SMALLINT")
          case "INT" => sb.append(" INT")
          case _ => sb.append(" STRING")
        }
        sb.append(",")
      }
    }
    sb.deleteCharAt(sb.size-1)
    sb.append(") partitioned by (city String, logdate string, hour string) ")
    sb.append(s"stored as ORC location '$tb_location' ")
    sb.append("tblproperties(\"orc.compression\"=\""+tb_compress.toUpperCase+"\")")

    sb.toString()
  }
}
