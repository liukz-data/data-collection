package com.hyyx

import java.io.{FileNotFoundException, IOException}
import java.net.URI
import java.util.Date
import com.table.MroTable.hadoopConf
import com.util.LogToPgDBTool
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object TestFileSystem {
  val hadoopConf = new Configuration()
  def getHadoopConf(hadoopConfMap: Map[String, String]):Configuration={

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
    hadoopConf
  }




}
