package com.testpg

import java.text.SimpleDateFormat
import java.util.Date

import com.util.{DecryptTool, LogToPgDBTool, PgMergeLogicTb}

import scala.collection.mutable

object TestPg2 {

  def main(args: Array[String]): Unit = {
    PgMergeLogicTb.init("10.254.222.226", 5432, "zhangyan", DecryptTool.getDecryptString("kKRLVHNfR554Ihda6uHhRg=="), "mlogdb")
    PgMergeLogicTb.insert(1,"test","test","test","test","test")
    PgMergeLogicTb.end()
  }
}
