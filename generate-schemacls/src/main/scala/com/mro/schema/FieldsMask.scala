package com.mro.schema

//import java.text.SimpleDateFormat
//import java.util.{Date, List}
// import com.google.gson.JsonSyntaxException
import java.io.ByteArrayOutputStream
import java.net.URI
import java.util

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

import scala.collection.mutable

object FieldsMask {

//  private val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private def getFiledsMask(jsonStr: String, beanCls: Class[JsonBean]): Map[String, (Boolean, String)] = {
    // 初始化字段Map，将所有字段key的value初始化为(false,null)
    val result = mutable.HashMap[String, (Boolean, String)](
      "ENBID" -> (false,null), "ECI" -> (false,null), "CGI" -> (false,null),
      "MmeUeS1apId" -> (false,null), "MmeGroupId" -> (false,null), "MmeCode" -> (false,null), "TimeStamp" -> (false,null),
      "LteScEarfcn" -> (false,null), "LteScPci" -> (false,null), "LteScRSRP" -> (false,null),
      "LteSceNBRxTxTimeDiff" -> (false,null), "LteScTadv" -> (false,null), "LteScRSRQ" -> (false,null),
      "LteScPUSCHPRBNum" -> (false,null), "LteScPHR" -> (false,null), "LteScAOA" -> (false,null),
      "LteScSinrUL" -> (false,null), "LteScRI1" -> (false,null), "LteScRI2" -> (false,null),
      "LteScRI4" -> (false,null), "LteScRI8" -> (false,null), "LteScBSR" -> (false,null), "LteScPDSCHPRBNum" -> (false,null),
      "LteNcEarfcn01" -> (false,null), "LteNcPci01" -> (false,null), "LteNcRSRP01" -> (false,null), "LteNcRSRQ01" -> (false,null),
      "TdsNcellUarfcn01" -> (false,null), "TdsCellParameterId01" -> (false,null), "TdsPccpchRSCP01" -> (false,null),
      "GsmNcellNcc01" -> (false,null), "GsmNcellBcc01" -> (false,null), "GsmNcellBcch01" -> (false,null), "GsmNcellCarrierRSSI01" -> (false,null),
      "LteNcEarfcn02" -> (false,null), "LteNcPci02" -> (false,null), "LteNcRSRP02" -> (false,null), "LteNcRSRQ02" -> (false,null),
      "TdsNcellUarfcn02" -> (false,null), "TdsCellParameterId02" -> (false,null), "TdsPccpchRSCP02" -> (false,null),
      "GsmNcellNcc02" -> (false,null), "GsmNcellBcc02" -> (false,null), "GsmNcellBcch02" -> (false,null), "GsmNcellCarrierRSSI02" -> (false,null),
      "LteNcEarfcn03" -> (false,null), "LteNcPci03" -> (false,null), "LteNcRSRP03" -> (false,null), "LteNcRSRQ03" -> (false,null),
      "TdsNcellUarfcn03" -> (false,null), "TdsCellParameterId03" -> (false,null), "TdsPccpchRSCP03" -> (false,null),
      "GsmNcellNcc03" -> (false,null), "GsmNcellBcc03" -> (false,null), "GsmNcellBcch03" -> (false,null), "GsmNcellCarrierRSSI03" -> (false,null),
      "LteNcEarfcn04" -> (false,null), "LteNcPci04" -> (false,null), "LteNcRSRP04" -> (false,null), "LteNcRSRQ04" -> (false,null),
      "TdsNcellUarfcn04" -> (false,null), "TdsCellParameterId04" -> (false,null), "TdsPccpchRSCP04" -> (false,null),
      "GsmNcellNcc04" -> (false,null), "GsmNcellBcc04" -> (false,null), "GsmNcellBcch04" -> (false,null), "GsmNcellCarrierRSSI04" -> (false,null),
      "LteNcEarfcn05" -> (false,null), "LteNcPci05" -> (false,null), "LteNcRSRP05" -> (false,null), "LteNcRSRQ05" -> (false,null),
      "TdsNcellUarfcn05" -> (false,null), "TdsCellParameterId05" -> (false,null), "TdsPccpchRSCP05" -> (false,null),
      "GsmNcellNcc05" -> (false,null), "GsmNcellBcc05" -> (false,null), "GsmNcellBcch05" -> (false,null), "GsmNcellCarrierRSSI05" -> (false,null),
      "LteNcEarfcn06" -> (false,null), "LteNcPci06" -> (false,null), "LteNcRSRP06" -> (false,null), "LteNcRSRQ06" -> (false,null),
      "TdsNcellUarfcn06" -> (false,null), "TdsCellParameterId06" -> (false,null), "TdsPccpchRSCP06" -> (false,null),
      "GsmNcellNcc06" -> (false,null), "GsmNcellBcc06" -> (false,null), "GsmNcellBcch06" -> (false,null), "GsmNcellCarrierRSSI06" -> (false,null),
      "LteNcEarfcn07" -> (false,null), "LteNcPci07" -> (false,null), "LteNcRSRP07" -> (false,null), "LteNcRSRQ07" -> (false,null),
      "TdsNcellUarfcn07" -> (false,null), "TdsCellParameterId07" -> (false,null), "TdsPccpchRSCP07" -> (false,null),
      "GsmNcellNcc07" -> (false,null), "GsmNcellBcc07" -> (false,null), "GsmNcellBcch07" -> (false,null), "GsmNcellCarrierRSSI07" -> (false,null),
      "LteNcEarfcn08" -> (false,null), "LteNcPci08" -> (false,null), "LteNcRSRP08" -> (false,null), "LteNcRSRQ08" -> (false,null),
      "TdsNcellUarfcn08" -> (false,null), "TdsCellParameterId08" -> (false,null), "TdsPccpchRSCP08" -> (false,null),
      "GsmNcellNcc08" -> (false,null), "GsmNcellBcc08" -> (false,null), "GsmNcellBcch08" -> (false,null), "GsmNcellCarrierRSSI08" -> (false,null),
      "LteNcEarfcn09" -> (false,null), "LteNcPci09" -> (false,null), "LteNcRSRP09" -> (false,null), "LteNcRSRQ09" -> (false,null),
      "TdsNcellUarfcn09" -> (false,null), "TdsCellParameterId09" -> (false,null), "TdsPccpchRSCP09" -> (false,null),
      "GsmNcellNcc09" -> (false,null), "GsmNcellBcc09" -> (false,null), "GsmNcellBcch09" -> (false,null), "GsmNcellCarrierRSSI09" -> (false,null),
      "LteNcEarfcn10" -> (false,null), "LteNcPci10" -> (false,null), "LteNcRSRP10" -> (false,null), "LteNcRSRQ10" -> (false,null),
      "TdsNcellUarfcn10" -> (false,null), "TdsCellParameterId10" -> (false,null), "TdsPccpchRSCP10" -> (false,null),
      "GsmNcellNcc10" -> (false,null), "GsmNcellBcc10" -> (false,null), "GsmNcellBcch10" -> (false,null), "GsmNcellCarrierRSSI10" -> (false,null),
      "LteNcEarfcn11" -> (false,null), "LteNcPci11" -> (false,null), "LteNcRSRP11" -> (false,null), "LteNcRSRQ11" -> (false,null),
      "TdsNcellUarfcn11" -> (false,null), "TdsCellParameterId11" -> (false,null), "TdsPccpchRSCP11" -> (false,null),
      "GsmNcellNcc11" -> (false,null), "GsmNcellBcc11" -> (false,null), "GsmNcellBcch11" -> (false,null), "GsmNcellCarrierRSSI11" -> (false,null),
      "LteNcEarfcn12" -> (false,null), "LteNcPci12" -> (false,null), "LteNcRSRP12" -> (false,null), "LteNcRSRQ12" -> (false,null),
      "TdsNcellUarfcn12" -> (false,null), "TdsCellParameterId12" -> (false,null), "TdsPccpchRSCP12" -> (false,null),
      "GsmNcellNcc12" -> (false,null), "GsmNcellBcc12" -> (false,null), "GsmNcellBcch12" -> (false,null), "GsmNcellCarrierRSSI12" -> (false,null),
      "LteScPlrULQci1" -> (false,null), "LteScPlrULQci2" -> (false,null), "LteScPlrULQci3" -> (false,null),
      "LteScPlrULQci4" -> (false,null), "LteScPlrULQci5" -> (false,null), "LteScPlrULQci6" -> (false,null),
      "LteScPlrULQci7" -> (false,null), "LteScPlrULQci8" -> (false,null), "LteScPlrULQci9" -> (false,null),
      "LteScPlrDLQci1" -> (false,null), "LteScPlrDLQci2" -> (false,null), "LteScPlrDLQci3" -> (false,null),
      "LteScPlrDLQci4" -> (false,null), "LteScPlrDLQci5" -> (false,null), "LteScPlrDLQci6" -> (false,null),
      "LteScPlrDLQci7" -> (false,null), "LteScPlrDLQci8" -> (false,null), "LteScPlrDLQci9" -> (false,null),
      "LteScRIP2" -> (false,null), "LteScRIP7" -> (false,null)
    )

    try{
      val gson = new Gson()
      val jsonBean = gson.fromJson(jsonStr, beanCls)
      import scala.collection.JavaConversions._

      for (field <- jsonBean.mro_key.fields) {
        result.put(field.name, (true, field.mold))
      }
      for (field <- jsonBean.mro_sc.fields) {
        result.put(field.name, (true, field.mold))
      }
      for (field <- jsonBean.mro_nc.fields; num <- 1 to jsonBean.mro_nc.nc_num) {
        result.put(field.name + "%02d".format(num), (true, field.mold))
      }
      for (field <- jsonBean.mro_2nd.fields) {
        result.put(field.name, (true, field.mold))
      }
      for (field <- jsonBean.mro_3nd.fields) {
        result.put(field.name, (true, field.mold))
      }
      result.toMap
    } catch {
//      case ex: JsonSyntaxException =>
//        throw new Exception(ex)
//        System.err.println(sm.format(new Date()) + " ERROR FieldsMask: 日志字段配置信息的json文件中有json语法错误！")
//        ex.printStackTrace()
//        null
      case ex: Exception =>
        throw ex
//        System.err.println(sm.format(new Date()) + " ERROR FieldsMask: 读取日志字段配置信息的json文件时遇到问题！")
//        ex.printStackTrace()
        null
    } finally {

    }

  }

  private var mask: Map[String, (Boolean, String)] = _

  def allFieldsMask(pJsonStr: String = null): Map[String, (Boolean, String)] = {
    if (mask!=null)
      return mask

    var jsonStr = System.getProperty("SYS_MROOBJ_JSON")
    if (jsonStr == null) {
      jsonStr = pJsonStr
     if (jsonStr == null) {
       jsonStr = this.getContent("/mro/property/mro_field.json")
        if (jsonStr == null) {
          throw new Exception("无法读取到字段信息的json文件/tmp/prop/mro_field.json的内容")
        }
      }
    }
    mask = getFiledsMask(jsonStr, classOf[JsonBean])
    mask
  }

//  def clear: Unit ={
//    // 测试用
//    mask = null
//  }

  val allFields: Seq[String] =Seq("ENBID", "ECI", "CGI", "MmeUeS1apId", "MmeGroupId", "MmeCode", "TimeStamp",
      "LteScEarfcn", "LteScPci", "LteScRSRP", "LteSceNBRxTxTimeDiff", "LteScTadv", "LteScRSRQ", "LteScPUSCHPRBNum",
      "LteScPHR", "LteScAOA", "LteScSinrUL", "LteScRI1", "LteScRI2", "LteScRI4", "LteScRI8", "LteScBSR", "LteScPDSCHPRBNum",
      "LteNcEarfcn01", "LteNcPci01", "LteNcRSRP01", "LteNcRSRQ01", "TdsNcellUarfcn01", "TdsCellParameterId01",
      "TdsPccpchRSCP01", "GsmNcellNcc01", "GsmNcellBcc01", "GsmNcellBcch01", "GsmNcellCarrierRSSI01",
      "LteNcEarfcn02", "LteNcPci02", "LteNcRSRP02", "LteNcRSRQ02", "TdsNcellUarfcn02", "TdsCellParameterId02",
      "TdsPccpchRSCP02", "GsmNcellNcc02", "GsmNcellBcc02", "GsmNcellBcch02", "GsmNcellCarrierRSSI02",
      "LteNcEarfcn03", "LteNcPci03", "LteNcRSRP03", "LteNcRSRQ03", "TdsNcellUarfcn03", "TdsCellParameterId03",
      "TdsPccpchRSCP03", "GsmNcellNcc03", "GsmNcellBcc03", "GsmNcellBcch03", "GsmNcellCarrierRSSI03",
      "LteNcEarfcn04", "LteNcPci04", "LteNcRSRP04", "LteNcRSRQ04", "TdsNcellUarfcn04", "TdsCellParameterId04",
      "TdsPccpchRSCP04", "GsmNcellNcc04", "GsmNcellBcc04", "GsmNcellBcch04", "GsmNcellCarrierRSSI04",
      "LteNcEarfcn05", "LteNcPci05", "LteNcRSRP05", "LteNcRSRQ05", "TdsNcellUarfcn05", "TdsCellParameterId05",
      "TdsPccpchRSCP05", "GsmNcellNcc05", "GsmNcellBcc05", "GsmNcellBcch05", "GsmNcellCarrierRSSI05",
      "LteNcEarfcn06", "LteNcPci06", "LteNcRSRP06", "LteNcRSRQ06", "TdsNcellUarfcn06", "TdsCellParameterId06",
      "TdsPccpchRSCP06", "GsmNcellNcc06", "GsmNcellBcc06", "GsmNcellBcch06", "GsmNcellCarrierRSSI06",
      "LteNcEarfcn07", "LteNcPci07", "LteNcRSRP07", "LteNcRSRQ07", "TdsNcellUarfcn07", "TdsCellParameterId07",
      "TdsPccpchRSCP07", "GsmNcellNcc07", "GsmNcellBcc07", "GsmNcellBcch07", "GsmNcellCarrierRSSI07",
      "LteNcEarfcn08", "LteNcPci08", "LteNcRSRP08", "LteNcRSRQ08", "TdsNcellUarfcn08", "TdsCellParameterId08",
      "TdsPccpchRSCP08", "GsmNcellNcc08", "GsmNcellBcc08", "GsmNcellBcch08", "GsmNcellCarrierRSSI08",
      "LteNcEarfcn09", "LteNcPci09", "LteNcRSRP09", "LteNcRSRQ09", "TdsNcellUarfcn09", "TdsCellParameterId09",
      "TdsPccpchRSCP09", "GsmNcellNcc09", "GsmNcellBcc09", "GsmNcellBcch09", "GsmNcellCarrierRSSI09",
      "LteNcEarfcn10", "LteNcPci10", "LteNcRSRP10", "LteNcRSRQ10", "TdsNcellUarfcn10", "TdsCellParameterId10",
      "TdsPccpchRSCP10", "GsmNcellNcc10", "GsmNcellBcc10", "GsmNcellBcch10", "GsmNcellCarrierRSSI10",
      "LteNcEarfcn11", "LteNcPci11", "LteNcRSRP11", "LteNcRSRQ11", "TdsNcellUarfcn11", "TdsCellParameterId11",
      "TdsPccpchRSCP11", "GsmNcellNcc11", "GsmNcellBcc11", "GsmNcellBcch11", "GsmNcellCarrierRSSI11",
      "LteNcEarfcn12", "LteNcPci12", "LteNcRSRP12", "LteNcRSRQ12", "TdsNcellUarfcn12", "TdsCellParameterId12",
      "TdsPccpchRSCP12", "GsmNcellNcc12", "GsmNcellBcc12", "GsmNcellBcch12", "GsmNcellCarrierRSSI12",
      "LteScPlrULQci1", "LteScPlrULQci2", "LteScPlrULQci3", "LteScPlrULQci4", "LteScPlrULQci5",
      "LteScPlrULQci6", "LteScPlrULQci7", "LteScPlrULQci8", "LteScPlrULQci9",
      "LteScPlrDLQci1", "LteScPlrDLQci2", "LteScPlrDLQci3", "LteScPlrDLQci4", "LteScPlrDLQci5",
      "LteScPlrDLQci6", "LteScPlrDLQci7", "LteScPlrDLQci8", "LteScPlrDLQci9",
      "LteScRIP2", "LteScRIP7")

  private val hadoopConf = new Configuration()

  private def getContent(path: String): String = {
    var fs: FileSystem = null
    try {
      fs = FileSystem.get(URI.create(path), hadoopConf)
      val is = fs.open(new Path(path))
      val baos = new ByteArrayOutputStream(4096)
      IOUtils.copyBytes(is, baos, hadoopConf)
      baos.toString
    } catch {
      case ex: Exception =>
        val e = new Exception(
          String.join("\n",
            s"读取HDFS的文件 $path 时发生错误",
            ex.getMessage + {
              if (ex.getCause == null) "" else "\n" + ex.getCause.toString
            }
          )
        )
        e.setStackTrace(ex.getStackTrace)
        throw e
        null
    } finally {
      //IOUtils.closeStream(fs)
    }
  }

  def main(args: Array[String]): Unit = {
    val fieldsMap = getFiledsMask(
      args(0), // json文件位置
      classOf[JsonBean])
    println(fieldsMap)
  }
}

class Fields(var name: String, var mold: String) {
  def getName: String = name

  def setName(name: String): Unit = {
    this.name = name
  }

  def getMold: String = mold

  def setMold(mold: String): Unit = {
    this.mold = mold
  }
}

class Mro_key(var sequence: Byte, var fields: util.List[Fields], var nc_num: Byte) {
  def getSequence: Byte = sequence

  def setSequence(sequence: Byte): Unit = {
    this.sequence = sequence
  }

  def getFields: util.List[Fields] = fields

  def setFields(fields: util.List[Fields]): Unit = {
    this.fields = fields
  }
}

class Mro_sc(var sequence: Byte, var fields: util.List[Fields], var nc_num: Byte) {
  def getSequence: Byte = sequence

  def setSequence(sequence: Byte): Unit = {
    this.sequence = sequence
  }

  def getFields: util.List[Fields] = fields

  def setFields(fields: util.List[Fields]): Unit = {
    this.fields = fields
  }
}

class Mro_nc(var sequence: Byte, var fields: util.List[Fields], var nc_num: Byte) {
  def getSequence: Byte = sequence

  def setSequence(sequence: Byte): Unit = {
    this.sequence = sequence
  }

  def getFields: util.List[Fields] = fields

  def setFields(fields: util.List[Fields]): Unit = {
    this.fields = fields
  }

  def getNc_num: Byte = nc_num

  def setNc_num(nc_num: Byte): Unit = {
    this.nc_num = nc_num
  }
}

class Mro_2nd(var sequence: Byte, var fields: util.List[Fields], var nc_num: Byte) {
  def getSequence: Byte = sequence

  def setSequence(sequence: Byte): Unit = {
    this.sequence = sequence
  }

  def getFields: util.List[Fields] = fields

  def setFields(fields: util.List[Fields]): Unit = {
    this.fields = fields
  }
}

class Mro_3nd(var sequence: Byte, var fields: util.List[Fields], var nc_num: Byte) {
  def getSequence: Byte = sequence

  def setSequence(sequence: Byte): Unit = {
    this.sequence = sequence
  }

  def getFields: util.List[Fields] = fields

  def setFields(fields: util.List[Fields]): Unit = {
    this.fields = fields
  }
}

class JsonBean(var mro_key: Mro_key, var mro_sc: Mro_sc, var mro_nc: Mro_nc, var mro_2nd: Mro_2nd, var mro_3nd: Mro_3nd) {
  def getMro_key: Mro_key = mro_key

  def setMro_key(mro_key: Mro_key): Unit = {
    this.mro_key = mro_key
  }

  def getMro_sc: Mro_sc = mro_sc

  def setMro_sc(mro_sc: Mro_sc): Unit = {
    this.mro_sc = mro_sc
  }

  def getMro_nc: Mro_nc = mro_nc

  def setMro_nc(mro_nc: Mro_nc): Unit = {
    this.mro_nc = mro_nc
  }

  def getMro_2nd: Mro_2nd = mro_2nd

  def setMro_2nd(mro_2nd: Mro_2nd): Unit = {
    this.mro_2nd = mro_2nd
  }

  def getMro_3nd: Mro_3nd = mro_3nd

  def setMro_3nd(mro_3nd: Mro_3nd): Unit = {
    this.mro_3nd = mro_3nd
  }
}