package com.hyyx

import java.io.{File, FileReader}
import java.net.URI
import java.util.Properties


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

import scala.collection.mutable
import scala.xml.XML

object Test1 {
  private val prop: Properties = new Properties()

  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org").setLevel(Level.ERROR)
    // 1. 获取自定义的配置信息
    prop.load(new FileReader(".\\conf\\mromerge-conf.properties"))

    // 2. 获取 Hadoop配置
    val hadoopConfMap = getHadoopConf().toMap

    if (prop.getProperty("hadoop_security_authentication", "simple")
      .toLowerCase == "kerberos") {
      System.setProperty("java.security.krb5.conf",
        prop.getProperty("krb5_conf", null))
      accessHadoop()
    }

    // 3. 调用测试主程序
    val conf = TestFileSystem.getHadoopConf(hadoopConfMap)
    val uri = URI.create("/test/test_lkz_other/data")
    val fs = FileSystem.get(conf)

    val fsContent = fs.getContentSummary(new Path("/test/test_lkz_other/data"))


    fs.listStatus(new Path("/test/test_lkz_other/comp_seqfiles"))

    println("fsContent:"+fsContent)
    val dirCount = fsContent.getLength
    println("dirCount:"+dirCount/(1024.00*1024))


    val compSeqFilePath = "/test/test_lkz_other/comp_seqfiles"
    val filesStatus = fs.listStatus(new Path(compSeqFilePath))

    val len1 = filesStatus.length
    if (len1 == 0) {
      null
    } else {
      println(" INFO MergeSeqFiles: 进行合并的数据文件属于以下分区：")
      val sb = new StringBuilder
      val arrCompSeqFile = new Array[String](len1)
      for (i <- 0 until len1) {
        arrCompSeqFile(i) = filesStatus(i).getPath.getName
        println(arrCompSeqFile(i))
        sb.append(arrCompSeqFile(i) + " \n")
      }
      if (sb.nonEmpty)
        sb.deleteCharAt(sb.size-1)
      println(arrCompSeqFile.toBuffer)
      val city_date_hour = arrCompSeqFile(0).split("_")
      val strJoin = String.join("/","/test/test_lkz_other/data",city_date_hour(0),city_date_hour(1),city_date_hour(2))
      println(strJoin)
      val fsContent = fs.getContentSummary(new Path(strJoin))
      println(fsContent.getLength/(1024*1024)/100)
    }
  }

  /**
    * 使合并数据的程序可以访问 启用了Kerberos的 CDH Hadoop集群
    */
  private def accessHadoop(): Unit ={
    val hive_user = prop.getProperty("hive_user")
    val hive_keytab = prop.getProperty("hive_keytab")
    val hadoopConf = new Configuration()
    hadoopConf.addResource(
      new File(".\\conf\\core-site.xml")
        .toURI.toURL)
    hadoopConf.addResource(
      new File(".\\conf\\hdfs-site.xml")
        .toURI.toURL)

    /*    if (prop.getProperty("hadoop_security_authentication", "simple")
          .toLowerCase == "kerberos") {
          System.setProperty("java.security.krb5.conf",
            prop.getProperty("krb5_conf", null))
        }*/
    try {
      UserGroupInformation.setConfiguration(hadoopConf)
      UserGroupInformation.loginUserFromKeytab(hive_user, hive_keytab)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        System.exit(1)
    }
  }

  /**
    * 将Hadoop *-site.xml解析为HashMap返回
    * 本地测试用，提交到CDH集群后就不用在代码中进行加载了
    * @return
    */
  private def getHadoopConf(): mutable.Map[String, String] = {
    // 获取 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
    val hiveConfMap = parseXMLToMap(
      ".\\conf\\hive-site.xml")
    var hadoopConfMap = mutable.HashMap.empty[String, String]
    hadoopConfMap ++= parseXMLToMap(
      ".\\conf\\core-site.xml")
    hadoopConfMap ++= parseXMLToMap(
      ".\\conf\\hdfs-site.xml")
    hadoopConfMap ++= parseXMLToMap(
      ".\\conf\\mapred-site.xml")
    hadoopConfMap ++= parseXMLToMap(
      ".\\conf\\yarn-site.xml")
    //    hadoopConfMap.foreach(x=>println(x._1 + " -> " + x._2))
    hadoopConfMap ++= hiveConfMap
    hadoopConfMap
  }

  /**
    * 将Hadoop *-site.xml的配置内容解析为HashMap
    * 本地测试用，提交到CDH集群后就不用在代码中进行加载了
    * @param xmlFilePath
    * @return
    */
  private def parseXMLToMap(xmlFilePath: String): Map[String, String] = {
    val confMap = new mutable.HashMap[String, String]
    val someXml = XML.loadFile(xmlFilePath)

    // 对于 core-site.xml、hdfs-site.xml、mapred-site.xml、yarn-site.xml
    // 需要在配置项的名字前加上 "spark.hadoop."
    val filename = xmlFilePath.substring(xmlFilePath.lastIndexOf(File.separator)+1)
    if (filename=="core-site.xml" || filename=="hdfs-site.xml"
      || filename=="mapred-site.xml" || filename=="yarn-site.xml" ) {
      (someXml \\ "property").foreach(item=>{
        confMap.put( "spark.hadoop." + (item \ "name").text, (item\"value").text )
      })
    } else {
      (someXml \\ "property").foreach(item=>{
        confMap.put( (item \ "name").text, (item\"value").text )
      })
    }

    confMap.toMap
  }

}
