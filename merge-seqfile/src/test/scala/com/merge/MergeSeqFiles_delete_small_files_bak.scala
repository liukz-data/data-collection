package com.merge

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileNotFoundException, IOException}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.mro.schema.{FieldsMask, MroObjectFieldsSeq}
import com.util.{DecryptTool, LogToPgDBTool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IOUtils, NullWritable}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable

object MergeSeqFiles_delete_small_files_bak{

  private val prop: Properties = new Properties()
  private var hadoopConf: Configuration = _
  private var fileSystem: FileSystem = _

  private val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  //1m
  private var tb_region_file_size = 1024*1024


  private val msgList2: mutable.ListBuffer[(String, String)] = new mutable.ListBuffer[(String, String)]

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
        System.err.println(sm.format(new Date()) + s" ERROR MergeSeqFiles: 读取文件 $path 时发生错误")
        msgList2.append((sm.format(new Date()), s"ERROR MergeSeqFiles: 读取文件 $path 时发生错误"))
        ex.printStackTrace()
        System.exit(1)
        null
    } finally {
      /*if (fs!=fileSystem)
        IOUtils.closeStream(fs)*/
    }
  }

  /**
    * 由 main 方法调用的实际数据文件合并的方法
    *
    * @param args args(0) -- 配置文件路径，args(1) -- Spark的日志级别
    */
  private[merge] def executeMerge(args: Array[String], hadoopConfMap: Map[String, String] = null): Unit = {
    println(sm.format(new Date()) + " INFO MergeSeqFiles: 开始执行 合并小数据文件的操作")
    msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 开始执行 合并小数据文件的操作"))

    var confFile: String = null
    var sparkLogLevel: String = "ERROR"
    if (args.length < 1) {
      System.err.println("使用方法: com.com.merge.MergeSeqFiles <配置文件路径> [<Spark日志级别，默认为error>]")
      System.exit(1)
    } else {
      confFile = args(0)
      if (args.length == 2)
        sparkLogLevel = args(1)
    }
    Logger.getLogger("org").setLevel(Level.toLevel(sparkLogLevel.toUpperCase, Level.ERROR))
    Logger.getLogger("hive").setLevel(Level.toLevel(sparkLogLevel.toUpperCase, Level.ERROR))

    // 0. 获取 Hadoop 配置
    println(sm.format(new Date()) + " INFO MergeSeqFiles: 获取 Hadoop Configuration 配置内容")
    msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 获取 Hadoop Configuration 配置内容"))
    hadoopConf = new Configuration()
    if (hadoopConfMap != null) {
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item => {
        if (item._1.startsWith("spark.hadoop.")) {
          hadoopConf.set(item._1.substring(13), item._2)
        }
      })
    }

    // 1. 读取 conf/mromerge-conf.properties 里的内容
    println(sm.format(new Date()) + s" INFO MergeSeqFiles: 读取配置文件 $confFile 里的内容")
    msgList2.append((sm.format(new Date()), s"INFO MergeSeqFiles: 读取配置文件 $confFile 里的内容"))

    try {
      val content = getContent(confFile)
      val bais = new ByteArrayInputStream(content.getBytes)
      prop.load(bais)
    } catch {
      case ioe: IOException =>
        System.err.println(sm.format(new Date()) +
          s" ERROR MergeSeqFiles: 读取配置文件 $confFile 的内容时遇到错误，程序异常退出！！！")
        ioe.printStackTrace()
        System.exit(1)
    }

    val pgdb_ip = prop.getProperty("pgdb_ip")
    val pgdb_port = prop.getProperty("pgdb_port")
    val pgdb_user = prop.getProperty("pgdb_user")
    val pgdb_passwd = prop.getProperty("pgdb_passwd")
    val pgdb_db = prop.getProperty("pgdb_db")

    if (pgdb_ip==null || pgdb_port==null || pgdb_user==null || pgdb_passwd==null || pgdb_db==null) {
      System.err.println(sm.format(new Date()) +
        s" ERROR MergeSeqFiles: 配置文件 $confFile 中PostgreSQL日志数据库信息缺失，程序异常退出！！！")
      System.exit(1)
    }
    try {
      LogToPgDBTool.init(pgdb_ip, pgdb_port.toInt, pgdb_user,
        DecryptTool.getDecryptString(pgdb_passwd), pgdb_db)
    } catch {
      case ex: Exception =>
        System.err.println(sm.format(new Date()) + " ERROR MergeSeqFiles " +
         String.join("\n    ",
          ex.getMessage + {
            if (ex.getCause == null) "" else "\n" + ex.getCause.toString
          }, ex.getStackTrace.mkString("\n    ")
        ))
        System.exit(1)
    }
    // 2. 获取已经完成输出的 SequenceFile的列表
    println(sm.format(new Date()) + " INFO MergeSeqFiles: 获取已经完成输出的 SequenceFile小文件的列表")
    msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 获取已经完成输出的 SequenceFile小文件的列表"))

    //得到的单位为m
    tb_region_file_size = prop.getProperty("tb_region_file_size").toInt*1024*1024
    val comp_seqfiles = prop.getProperty("comp_seqfiles")

    // 创建 FileSystem 实例
    fileSystem = FileSystem.get(URI.create(comp_seqfiles), this.hadoopConf)
    val seqfile_root = prop.getProperty("seqfile_root")
    val output_seqfiles = getCompletedSeqFiles(comp_seqfiles,seqfile_root)


    if (output_seqfiles == null) {
      System.out.println(sm.format(new Date()) + " INFO MergeSeqFiles: 没有某地市在某小时内的数据已完成输出")
      msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 没有某地市在某小时内的数据已完成输出"))
      LogToPgDBTool.execBatch2(msgList2)
      LogToPgDBTool.end()
      System.exit(0)
    }

    // 3. 读取并解析 日志字段配置信息的json文件 的内容，生成 FieldsMask.allFieldsMask
    // 获取 日志字段json文件位置，并通过System.setProperty方法 传入 FieldsMask
    System.out.println(sm.format(new Date()) +
      " INFO MergeSeqFiles: 读取"+prop.getProperty("mroobj_json")+"的内容")
    msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 读取"+prop.getProperty("mroobj_json")+"的内容"))

    val jsonContent: String = getContent(prop.getProperty("mroobj_json"))
//    System.setProperty("SYS_MROOBJ_JSON", jsonContent)
    // 先判断 日志字段配置信息的json文件 文件是否能被正确解析，不能则退出程序
    try {
      FieldsMask.allFieldsMask(jsonContent) == null
    } catch {
      case ex: Exception =>
        System.err.println(sm.format(new Date()) +
          " ERROR MergeSeqFiles: 日志字段配置信息json文件的内容没有被正确解析，数据合并程序退出！")
        ex.printStackTrace()
        msgList2.append((sm.format(new Date()),
          "ERROR MergeSeqFiles: 日志字段配置信息json文件的内容没有被正确解析，数据合并程序退出！"))
        LogToPgDBTool.execBatch2(msgList2)
        LogToPgDBTool.end()
        System.exit(1)
    }

    if (!LogToPgDBTool.execBatch2(msgList2)) {
      System.err.println(sm.format(new Date()) +
        " ERROR MergeSeqFiles: 产生的日志不能输出到PostgreSQL数据库中，数据合并程序退出！")
      System.err.println(LogToPgDBTool.getExceptionMsg)
      LogToPgDBTool.end()
      System.exit(1)
    }
    // 4. 创建SparkSQL的运行环境
    // 创建或获取 SparkConf、SparkSession、SparkContext
    val sparkConf = new SparkConf().setAppName("MergeSeqFiles")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.exec.orc.compression.strategy", "COMPRESSION")
      .set("hive.exec.orc.default.compress",
        prop.getProperty("tb_compress","SNAPPY").toUpperCase )

    if (hadoopConfMap != null) {
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item=>{
        sparkConf.set(item._1, item._2)
      })
      // 本地测试用，设置 Master，默认为 local[2]
      sparkConf.setMaster(hadoopConfMap.getOrElse("spark_master", "local[4]"))
    }

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    // 将字段信息json文件的内容在Spark中广播出去
    val jsonStrBC = sc.broadcast(jsonContent)
    // import spark.implicits._
    //    spark.sql("SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    //    spark.sql("SET mapreduce.output.fileoutputformat.compress.type=BLOCK")
    //    spark.sql("set hive.exec.compress.intermediate=true")
    //    spark.sql("set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
    //    spark.sql("SET hive.intermediate.compression.type=BLOCK")
    //    spark.sql("set hive.exec.orc.compression.strategy=COMPRESSION")
    //    spark.sql("set hive.exec.orc.default.compress=SNAPPY")

    // 5. 根据日志字段信息json文件，设定Spark Sql的schema
    val schema: StructType = getFieldsSchema


    // 6.将每一组输出完毕的 SequenceFile 从目录中读出，合并至Hive ORC表中
    // 获取相关配置信息

    val tb_name = prop.getProperty("dbname", "mro") +
      "." + prop.getProperty("tb_name", "mro_dt_rec")

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
//    def getMroObjRDDDyn(sc: SparkContext,
//                        sfRoot: String, city: String, date: String, hour: String): RDD[Row] = {
////      System.setProperty("SYS_MROOBJ_JSON", jsonStrBC.value)
////      println("getMroObjRDDDyn start " + jsonContent.substring(0, 100))
////      FieldsMask.allFieldsMask(jsonStrBC.value)
//      val logRdd: RDD[MroObjectFieldsSeq] =
//        sc.sequenceFile(String.join("/", sfRoot, city, date, hour),
//          classOf[NullWritable],
//          classOf[MroObjectFieldsSeq], 1).map(row => row._2)
//
//      val tmpRdd = logRdd.map(line => {
////        println("getMroObjRDDDyn rdd map")
////        FieldsMask.allFieldsMask(jsonStrBC.value)
////        FieldsMask.allFieldsMask(sc.getConf.get("mrofields.json"))
//        val schema: Array[Any] =
//          new Array[Any](FieldsMask.allFieldsMask(jsonStrBC.value).count(item => item._2._1) + 3)
//
//        var schemaFieldsInd: Int = 0
//        if (FieldsMask.allFieldsMask().getOrElse("ENBID", (false, null))._1) {
//          schema(schemaFieldsInd) = line.ENBID
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//        if (FieldsMask.allFieldsMask().getOrElse("ECI", (false, null))._1) {
//          schema(schemaFieldsInd) = line.ECI
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//        if (FieldsMask.allFieldsMask().getOrElse("CGI", (false, null))._1) {
//          schema(schemaFieldsInd) = line.CGI
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//        if (FieldsMask.allFieldsMask().getOrElse("MmeUeS1apId", (false, null))._1) {
//          // 如何将scala.math.BigInt转换成为 org.apache.spark.sql.types.DecimalType
//          schema(schemaFieldsInd) = new java.math.BigDecimal(line.MmeUeS1apId.bigInteger)
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//        if (FieldsMask.allFieldsMask().getOrElse("MmeGroupId", (false, null))._1) {
//          schema(schemaFieldsInd) = line.MmeGroupId
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//        if (FieldsMask.allFieldsMask().getOrElse("MmeCode", (false, null))._1) {
//          schema(schemaFieldsInd) = line.MmeCode
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//        if (FieldsMask.allFieldsMask().getOrElse("TimeStamp", (false, null))._1) {
//          schema(schemaFieldsInd) = line.TimeStamp
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//        for (i <- line.fields.indices) {
//          schema(schemaFieldsInd) = line.fields(i)
//          schemaFieldsInd = schemaFieldsInd + 1
//        }
//
//        schema(schemaFieldsInd) = city
//        schema(schemaFieldsInd + 1) = date
//        schema(schemaFieldsInd + 2) = hour
//
//        Row.fromSeq(schema.toSeq)
//      })
//
//      tmpRdd
//    }

    output_seqfiles.par.foreach(compFileAndRegionNum => {
      val compFile = compFileAndRegionNum._1
      val regionNum = compFileAndRegionNum._2
      val msgList3: mutable.ListBuffer[(String, String, String)] = new mutable.ListBuffer[(String, String, String)]

      try {
        // 读取每一组输出完毕的 SequenceFile，创建这一组数据的RDD
        val city_date_hour = compFile.split("_")

        println(sm.format(new Date()) + s" INFO MergeSeqFiles: 创建 $compFile 的SequenceFile小文件的RDD")
        msgList3.append((sm.format(new Date()), compFile,
          s"INFO MergeSeqFiles: 创建 $compFile 的SequenceFile小文件的RDD"))

//        val mroObjRDD = getMroObjRDDDyn(sc, seqfile_root, city_date_hour(0), city_date_hour(1), city_date_hour(2))
        val mroObjRDD = {
          //      System.setProperty("SYS_MROOBJ_JSON", jsonStrBC.value)
          //      println("getMroObjRDDDyn start " + jsonContent.substring(0, 100))
          //      FieldsMask.allFieldsMask(jsonStrBC.value)
          val logRdd: RDD[MroObjectFieldsSeq] =
          sc.sequenceFile(String.join("/", seqfile_root,
            city_date_hour(0), city_date_hour(1), city_date_hour(2)),
            classOf[NullWritable],
            classOf[MroObjectFieldsSeq], 1).map(row => row._2)

          val tmpRdd = logRdd.map(line => {
            //        println("getMroObjRDDDyn rdd map")
            //        FieldsMask.allFieldsMask(jsonStrBC.value)
            //        FieldsMask.allFieldsMask(sc.getConf.get("mrofields.json"))
            val schema: Array[Any] =
            new Array[Any](FieldsMask.allFieldsMask(jsonStrBC.value).count(item => item._2._1) + 3)

            var schemaFieldsInd: Int = 0
            if (FieldsMask.allFieldsMask().getOrElse("ENBID", (false, null))._1) {
              schema(schemaFieldsInd) = line.ENBID
              schemaFieldsInd = schemaFieldsInd + 1
            }
            if (FieldsMask.allFieldsMask().getOrElse("ECI", (false, null))._1) {
              schema(schemaFieldsInd) = line.ECI
              schemaFieldsInd = schemaFieldsInd + 1
            }
            if (FieldsMask.allFieldsMask().getOrElse("CGI", (false, null))._1) {
              schema(schemaFieldsInd) = line.CGI
              schemaFieldsInd = schemaFieldsInd + 1
            }
            if (FieldsMask.allFieldsMask().getOrElse("MmeUeS1apId", (false, null))._1) {
              // 如何将scala.math.BigInt转换成为 org.apache.spark.sql.types.DecimalType
              schema(schemaFieldsInd) = new java.math.BigDecimal(line.MmeUeS1apId.bigInteger)
              schemaFieldsInd = schemaFieldsInd + 1
            }
            if (FieldsMask.allFieldsMask().getOrElse("MmeGroupId", (false, null))._1) {
              schema(schemaFieldsInd) = line.MmeGroupId
              schemaFieldsInd = schemaFieldsInd + 1
            }
            if (FieldsMask.allFieldsMask().getOrElse("MmeCode", (false, null))._1) {
              schema(schemaFieldsInd) = line.MmeCode
              schemaFieldsInd = schemaFieldsInd + 1
            }
            if (FieldsMask.allFieldsMask().getOrElse("TimeStamp", (false, null))._1) {
              schema(schemaFieldsInd) = line.TimeStamp
              schemaFieldsInd = schemaFieldsInd + 1
            }
            for (i <- line.fields.indices) {
              schema(schemaFieldsInd) = line.fields(i)
              schemaFieldsInd = schemaFieldsInd + 1
            }

            schema(schemaFieldsInd) = city_date_hour(0)
            schema(schemaFieldsInd + 1) = city_date_hour(1)
            schema(schemaFieldsInd + 2) = city_date_hour(2)

            Row.fromSeq(schema.toSeq)
          })

          tmpRdd
        }

        println(sm.format(new Date()) + s" INFO MergeSeqFiles: 开始，合并 $compFile 的SequenceFile小文件并输出到Hive表")
        msgList3.append((sm.format(new Date()), compFile,
          s"INFO MergeSeqFiles: 开始，合并 $compFile 的SequenceFile小文件并输出到Hive表"))
        val mroDF = spark.createDataFrame(mroObjRDD, schema)
        mroDF.createOrReplaceTempView(compFile)
        val results = spark.sql("SELECT " + schema.fieldNames.mkString(",") + " FROM " + compFile)
        //    results.persist(StorageLevel.MEMORY_ONLY_SER)
          println("partitons:"+spark.conf.get("spark.sql.shuffle.partitions"))
          results.coalesce(regionNum).write
          .format("orc")
          .mode(SaveMode.Overwrite).insertInto(tb_name)
        println(sm.format(new Date()) + s" INFO MergeSeqFiles: 完成，合并 $compFile 的SequenceFile小文件并输出到Hive表")
        msgList3.append((sm.format(new Date()), compFile,
          s"INFO MergeSeqFiles: 完成，合并 $compFile 的SequenceFile小文件并输出到Hive表"))

        // 6. 删除表明 已经完成SequenceFile输出的文件
        fileSystem.delete(new Path(comp_seqfiles + "/" + compFile), false)
        println(sm.format(new Date()) + " INFO MergeSeqFiles: 删除表明已经完成SequenceFile输出的文件 " + compFile)
        msgList3.append((sm.format(new Date()), compFile,
          "INFO MergeSeqFiles: 删除表明已经完成SequenceFile输出的文件 " + compFile))
        // 7. 删除 已经完成合并的 SequenceFile小文件
        val seqFileLoc = seqfile_root + "/" + city_date_hour(0) + "/" + city_date_hour(1) + "/" + city_date_hour(2)
        fileSystem.delete(new Path(seqFileLoc), true)
        println(sm.format(new Date()) +
          s" INFO MergeSeqFiles: 删除已经完成合并的 $compFile 位于 $seqFileLoc 下的SequenceFile小文件")
        msgList3.append((sm.format(new Date()), compFile,
          s"INFO MergeSeqFiles: 删除已经完成合并的 $compFile 位于 $seqFileLoc 下的SequenceFile小文件"))
      } catch {
        case ex: Exception =>
          val exceptionMsg = String.join("\n    ",
            ex.getMessage + { if (ex.getCause==null) "" else "\n"+ex.getCause.toString },
            ex.getStackTrace.mkString("\n    "))
          if (! ex.isInstanceOf[org.apache.spark.SparkException]){
            System.err.println(sm.format(new Date()) +
              " ERROR MergeSeqFiles: 合并数据出现错误，数据合并程序退出！")
            System.err.println(exceptionMsg)
          }
          msgList3.append((sm.format(new Date()), compFile,
            "ERROR MergeSeqFiles: 合并数据出现错误，数据合并程序退出！\n"+exceptionMsg))
      } finally {
        LogToPgDBTool.execBatch3(msgList3)
      }
      //spark.stop()
     // IOUtils.closeStream(fileSystem)
    })

    println(sm.format(new Date()) + " INFO MergeSeqFiles: 执行结束")
    msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 执行结束"))
    LogToPgDBTool.execBatch2(msgList2)

    LogToPgDBTool.end()
  }

  /**
    * 获取已完成输出的SequenceFile的小文件。
    * 通过从compSeqFilePath这个HDFS路径下获取形式是 "地市_日期_小时"的空文件的文件名，以得知哪些小文件已经完成了输出
    *
    * @param compSeqFilePath 用来存放名为 city-date-hour 格式空文件的HDFS目录
    * @return
    */
  private def getCompletedSeqFiles(compSeqFilePath: String,seqfile_root:String): Array[(String,Int)] = {
    try {
      val filesStatus = fileSystem.listStatus(new Path(compSeqFilePath))
      val len = filesStatus.length

      if (len == 0) {
        null
      } else {
        println(sm.format(new Date()) + " INFO MergeSeqFiles: 进行合并的数据文件属于以下分区：")
        val sb = new StringBuilder
        var arrCompSeqFile = new Array[(String,Int)](len)
        for (i <- 0 until len) {
          val fileName = filesStatus(i).getPath.getName
          val city_date_hour = fileName.split("_")
          val seqfile_root_city_date_hour = String.join("/", seqfile_root,city_date_hour(0), city_date_hour(1), city_date_hour(2))
         //得到分区数据量大小，计算出将要输出到hive表，每个分区的文件数，此地方统计的sequencefile的文件大小，而非是orc的文件大小
          val fsContent = fileSystem.getContentSummary(new Path(seqfile_root_city_date_hour))
          //1048576是1M
          val regionNum = fsContent.getLength/tb_region_file_size+1
          println("regionNum:"+regionNum)
          arrCompSeqFile(i) =(fileName,regionNum.toInt)
          sb.append(fileName + " \n")
        }
        if (sb.nonEmpty)
          sb.deleteCharAt(sb.size-1)

        msgList2.append((sm.format(new Date()), "INFO MergeSeqFiles: 进行合并的数据文件属于以下分区：\n" + sb))

        arrCompSeqFile
      }
    } catch {
      case ilaex: IllegalArgumentException =>
        System.err.println(sm.format(new Date()) +
          " ERROR MergeSeqFiles: properties配置文件中，comp_seqfiles的内容为空或没有被设置，程序异常退出！请输入正确的HDFS路径")
        msgList2.append((sm.format(new Date()),
          "ERROR MergeSeqFiles: properties配置文件中，comp_seqfiles的内容为空或没有被设置，程序异常退出！请输入正确的HDFS路径"))
        LogToPgDBTool.execBatch2(msgList2)
        LogToPgDBTool.end()
        ilaex.printStackTrace()
        System.exit(1)
        null
      case fnfex: FileNotFoundException =>
        System.err.println(sm.format(new Date()) +
          " ERROR MergeSeqFiles: properties配置文件中，comp_seqfiles设置的路径在HDFS没有被找到，程序异常退出！请输入正确的HDFS路径")
        msgList2.append((sm.format(new Date()),
          "ERROR MergeSeqFiles: properties配置文件中，comp_seqfiles设置的路径在HDFS没有被找到，程序异常退出！请输入正确的HDFS路径"))
        LogToPgDBTool.execBatch2(msgList2)
        LogToPgDBTool.end()
        fnfex.printStackTrace()
        System.exit(1)
        null
      case ioex: IOException =>
        System.err.println(sm.format(new Date()) +
          " ERROR MergeSeqFiles: 在HDFS上读取properties配置文件中comp_seqfiles设置的路径，出现错误，程序异常退出！")
        msgList2.append((sm.format(new Date()),
          "ERROR MergeSeqFiles: 在HDFS上读取properties配置文件中comp_seqfiles设置的路径，出现错误，程序异常退出！"))
        LogToPgDBTool.execBatch2(msgList2)
        LogToPgDBTool.end()
        ioex.printStackTrace()
        System.exit(1)
        null
    }
  }



  /**
    * 根据日志字段信息的JSON配置文件，获取 SparkSQL 当前的Schema信息
    *
    * @return
    */
  private def getFieldsSchema: StructType = {
    // schema除了数据字段，还要加上city、logdate、hour三个分区字段
    val schema: Array[StructField] =
      new Array[StructField](FieldsMask.allFieldsMask().count(item => item._2._1) + 3)

    var schemaFieldsInd: Int = 0
    for (i <- FieldsMask.allFields.indices) {
      if (FieldsMask.allFieldsMask().getOrElse(FieldsMask.allFields(i), (false, null))._1) {
        val fieldTypeName: String =
          FieldsMask.allFieldsMask().getOrElse(FieldsMask.allFields(i), (false, null))._2.toUpperCase

        val fieldType: DataType = {
          fieldTypeName match {
            case "STRING" => DataTypes.StringType
            case "BIGINT" => DataTypes.createDecimalType(38, 0)
            case "BYTE" => DataTypes.ByteType
            case "SHORT" => DataTypes.ShortType
            case "INT" => DataTypes.IntegerType
            case _ => DataTypes.NullType
          }
        }
        schema(schemaFieldsInd) = StructField(FieldsMask.allFields(i), fieldType, nullable = false)
        schemaFieldsInd = schemaFieldsInd + 1
      }
    }

    schema(schemaFieldsInd) = StructField("city", StringType, nullable = false)
    schema(schemaFieldsInd + 1) = StructField("logdate", StringType, nullable = false)
    schema(schemaFieldsInd + 2) = StructField("hour", StringType, nullable = false)

    StructType(schema)
  }
}
