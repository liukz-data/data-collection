package com.testpg

import java.text.SimpleDateFormat
import java.util.Date
import com.util.{DecryptTool, LogToPgDBTool}

import scala.collection.mutable

object TestPg {

  def main(args: Array[String]): Unit = {
    LogToPgDBTool.init("10.254.222.226", 5432, "zhangyan",
      DecryptTool.getDecryptString("kKRLVHNfR554Ihda6uHhRg=="), "mlogdb")
    val msgList2: mutable.ListBuffer[(String, String)] = new mutable.ListBuffer[(String, String)]
    val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 没有某地市在某小时内的数据已完成输出"))
    LogToPgDBTool.execBatch2(msgList2)
    LogToPgDBTool.end()
  }
}
