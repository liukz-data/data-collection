package com.output

import java.io._
import java.net.URI
import java.util
import java.util.Properties

import com.cmdi.engine.mro.bean.MroKey
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.security.UserGroupInformation

object TestSeqFileOutput_main {

  private val prop: Properties = new Properties()

  def main(args: Array[String]): Unit = {

    // 1. 获取HDFS配置
    val hdfsConf = getHDFSConf

    // 2. 获取自定义的配置信息
    prop.load(new FileReader(
      "./conf/mromerge-conf.properties"))
    val seqfile_dir = prop.getProperty("seqfile_root")
    val comp_seqfiles = prop.getProperty("comp_seqfiles")

    // 3. 访问启用了Kerberos的HDFS
    if (prop.getProperty("hadoop_security_authentication", "simple")
      .toLowerCase == "kerberos") {
      System.setProperty("java.security.krb5.conf",
        prop.getProperty("krb5_conf", null))
      accessHdfs(hdfsConf)
    }

    // 获取 日志字段json文件的内容，并通过System.setProperty方法 传入 FieldsMask
    val content = getContent(prop.getProperty("mroobj_json"), hdfsConf)
    println(content)
    System.setProperty("SYS_MROOBJ_JSON", content)

    // 4. 读取样例数据文件，每一个文件生成一个 MroObject实例，形成一个含有MroObject实例数组
    // 数组元素为 Tuple(<路径>, <MroObject实例>)
    val dataDir = "./data_new"
//    val dataDir = ".\\partial-fields"
    val dataFiles = getCSVFiles(dataDir)
    println("------------------------------------------------")
    println(dataFiles(0))
    println("------------------------------------------------")
    val arrMroObject =
      for (dataFile <- dataFiles) yield {
//        (dataFile._1,
//        new GetMroObjFromFile(new InputStreamReader(new FileInputStream(dataFile._2))).getMroObj)
        val nameParts = dataFile.substring(dataDir.length+1, dataFile.length-4).split("_")
        ("/" + String.join("/",nameParts(0),
          nameParts(2).replaceAll("-",""),
          nameParts(3), nameParts(1)),
        new GetMroObjFromFile_main(new InputStreamReader(new FileInputStream(dataFile))).getMroObj)
      }

    // 5. 对每个MroObject实例应用 SeqFileOutput的方法
    // 获取当前HDFS的FileSystem实例
    val fileSystem = FileSystem.get(URI.create(hdfsConf.get("fs.defaultFS")), hdfsConf)

    import scala.collection.JavaConversions._

    arrMroObject.foreach(mroObjTup=>{

      // 应用 SeqFileOutput的方法 输出 SequenceFile到 HDFS上
      val result = SeqFileOutput.outputAsSequenceFile({
        val mroData: util.HashMap[MroKey, util.List[Object]] = new util.HashMap[MroKey, util.List[Object]]()
        val iter = mroObjTup._2.keysIterator
        while(iter.hasNext){
          val key = iter.next()
          val value = mroObjTup._2.get(key)
          mroData.put(key, value)
        }

        mroData
      }, seqfile_dir + mroObjTup._1, hdfsConf)
      println(s"$seqfile_dir${mroObjTup._1} 中的数据被转换成SequenceFile输出到HDFS上，" +
        {if(result==0) "成功" else "失败"})
      if (result!=0){
        System.err.println(SeqFileOutput.getExceptionMessage)
      }
    })

    // 6. SequenceFile 输出完毕后，向 HDFS的 comp_seqfiles 目录中创建名为 "city_date_hour" 的空文件
    fileSystem.create(new Path(comp_seqfiles + "/guiyang_20171108_00"), true).close()
    fileSystem.create(new Path(comp_seqfiles + "/zunyi_20171108_00"), true).close()
  }

  private def getHDFSConf: Configuration = {
    // 获取 HDFS Configuration实例
    val hdfsConf = new Configuration()
    hdfsConf.addResource(
      new File("./conf/hdfs-site.xml").toURI.toURL)
    hdfsConf.addResource(
      new File("./conf/core-site.xml").toURI.toURL)

    hdfsConf
  }

  private def accessHdfs(hdfsConf: Configuration): Unit = {
    val hive_user = prop.getProperty("hive_user")
    val hive_keytab = prop.getProperty("hive_keytab")

    try{
      UserGroupInformation.setConfiguration(hdfsConf)
      UserGroupInformation.loginUserFromKeytab(hive_user, hive_keytab)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        System.exit(1)
    }
  }

  private def getContent(path: String, hdfsConf: Configuration): String = {
    var fs: FileSystem = null
    try {
      fs = FileSystem.get(URI.create(path), hdfsConf)
      val is = fs.open(new Path(path))
      val baos = new ByteArrayOutputStream(4096)
      IOUtils.copyBytes(is, baos, hdfsConf)
      baos.toString
    } catch {
      case ex: Exception =>
        System.err.println(s" ERROR MergeSeqFiles: 读取文件 $path 时发生错误")
        ex.printStackTrace()
        System.exit(1)
        null
    } finally {
      IOUtils.closeStream(fs)
    }
  }

  private def getCSVFiles(dataDir: String): Array[String] = {
    new File(dataDir).listFiles().filter(_.getName.endsWith(".csv")).map(_.getPath)
  }

}
