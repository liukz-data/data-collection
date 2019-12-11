package com.mro.schema

import java.io.{DataInput, DataOutput}
//import java.text.SimpleDateFormat
//import java.util.Date

import org.apache.hadoop.io.Writable

/**
  * 实现了 Writable 接口，用于序列化和反序列化 日志内容
  */
class MroObjectFieldsSeq_int(
                          var ENBID: Int,
                          var ECI: Int,
                          var CGI: String,
                          var MmeUeS1apId: BigInt,
                          var MmeGroupId: Int,
                          var MmeCode: Int,
                          var TimeStamp: String,
                          var fields: Array[AnyVal]
                        ) extends Writable {
  /**
    * 重写无参构造函数,用于反序列化时的反射操作
    */
  def this() {
    this(
      0, 0, "", 0, 0, 0, "", new Array[AnyVal](MroObjectFieldsSeq_int.realFieldsLen)
    )
  }
  
//  private val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  
  override def write(dataOutput: DataOutput): Unit = {
    /**
      * 因为DataInput中只能靠 readUTF()把数据读取成 String，而 readLine() 只能靠换行符读取一整行的数据，
      * 所以写入 String 的话，要用 writeUTF()
      */
    // MroKey
//    if (FieldsMask.allFieldsMask.getOrElse("ENBID", (false, null))._1) {
      dataOutput.writeInt(ENBID)
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("ECI", (false, null))._1) {
      dataOutput.writeInt(ECI)
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("CGI", (false, null))._1) {
      val byteArrCGI = CGI.getBytes
      val lenCGI = byteArrCGI.length // CGI 的字节数组的长度 Short 类型
      dataOutput.writeInt(lenCGI)
      dataOutput.write(byteArrCGI)
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("MmeUeS1apId", (false, null))._1) {
      val byteArrMmeUeS1apId = MmeUeS1apId.toByteArray
      val lenMmeUeS1apId = byteArrMmeUeS1apId.length // MmeUeS1apId 的 长度 Short 类型
      dataOutput.writeInt(lenMmeUeS1apId)
      dataOutput.write(byteArrMmeUeS1apId)
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("MmeGroupId", (false, null))._1) {
      dataOutput.writeInt(MmeGroupId)
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("MmeCode", (false, null))._1) {
      dataOutput.writeInt(MmeCode)
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("TimeStamp", (false, null))._1) {
      val byteArrTimeStamp = TimeStamp.getBytes
      val lenTimeStamp = byteArrTimeStamp.length // TimeStamp 的 长度 Short 类型
      dataOutput.writeInt(lenTimeStamp)
      dataOutput.write(byteArrTimeStamp)
//    }

    // mrofields: mro_sc, mro_nc, mro_2nd, mro_3nd
    var fieldsInd: Int = 0
    for( i <- 7 until FieldsMask.allFields.length) {
      val (isOut, cls) = FieldsMask.allFieldsMask().getOrElse(FieldsMask.allFields(i), (false, null))
      if (isOut) {
        try {
          cls.toUpperCase match {
            case "INT" => dataOutput.writeInt(fields(fieldsInd).asInstanceOf[Int].intValue())
            case "SHORT" => dataOutput.writeShort(fields(fieldsInd).asInstanceOf[Short].intValue())
            case "BYTE" => dataOutput.writeByte(fields(fieldsInd).asInstanceOf[Byte].intValue())
            case _ =>
              throw new Exception("序列化时，日志字段信息的JSON文件描述有错误，问题字段是" + FieldsMask.allFields(i))
//              System.err.println(sm.format(new Date()) +
//                " ERROR MroObjectFieldsSeq: 序列化时，日志字段信息的JSON文件描述有错误，问题字段是" + FieldsMask.allFields(i))
          }
          fieldsInd = fieldsInd + 1
        } catch {
          case iobe: java.lang.IndexOutOfBoundsException =>
            val e = new Exception(
              "序列化过程中，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错数据的MroKey是：\n"
              + String.join(",", ENBID.toString, ECI.toString, CGI,
              MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp)
            )
            e.setStackTrace(iobe.getStackTrace)
            throw e
//            System.err.print(sm.format(new Date()) +
//              " ERROR MroObjectFieldsSeq: 序列化过程中，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错数据的MroKey是：")
//            System.err.println(
//              String.join(",", ENBID.toString, ECI.toString, CGI,
//                MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
//            iobe.printStackTrace(System.err)
//            System.exit(1)
          case ioe: java.io.IOException =>
            val e = new Exception("序列化数据时遇到错误。出错数据的MroKey是：\n"+
              String.join(",", ENBID.toString, ECI.toString, CGI,
                MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp)+
              "\n，出错的字段是：" + FieldsMask.allFields(i) + "，字段值是：" + fields(i))
            ioe.printStackTrace()
            e.setStackTrace(ioe.getStackTrace)
            throw e
//            System.err.print(sm.format(new Date()) +
//              " ERROR MroObjectFieldsSeq: ")
//            System.err.print(
//              String.join(",", ENBID.toString, ECI.toString, CGI,
//                MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
//            System.err.println("，出错的字段是：" + FieldsMask.allFields(i) + "，字段值是：" + fields(i))
//            ioe.printStackTrace()
//            System.exit(1)
          case ex: Exception =>
            val e = new Exception(String.join("\n",
              "序列化数据时遇到错误。出错数据的MroKey是：",
              String.join(",", ENBID.toString, ECI.toString, CGI,
                MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp),
                "出错的字段是：" + FieldsMask.allFields(i) + "，字段值是：" + fields(i),
              ex.getMessage))
            e.setStackTrace(ex.getStackTrace)
            throw e
//            ex.printStackTrace()
//            System.exit(1)
        }
      }
    }

    if ( fieldsInd != MroObjectFieldsSeq_int.realFieldsLen ){
      val e = new Exception("序列化数据时，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错的数据MroKey是：\n"
          + String.join(",", ENBID.toString, ECI.toString, CGI,
        MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
      throw e
//      System.err.println(sm.format(new Date()) +
//        " ERROR MroObjectFieldsSeq: 序列化数据时，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错的数据MroKey是：")
//      System.err.println(
//        String.join(",", ENBID.toString, ECI.toString, CGI,
//          MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
//      System.exit(1)
    }
  }

  override def readFields(dataInput: DataInput): Unit = {
    // mroKey
//    if (FieldsMask.allFieldsMask.getOrElse("ENBID", (false, null))._1) {
      ENBID = dataInput.readInt
//    } else {
//      ENBID = 0
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("ECI", (false, null))._1) {
      ECI = dataInput.readInt
//    } else {
//      ECI = 0
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("CGI", (false, null))._1) {
      val lenCGI = dataInput.readInt()
      val byteArrCGI = new Array[Byte](lenCGI)
      for (i <- 0 until lenCGI) {
        byteArrCGI(i) = dataInput.readByte()
      }
      CGI = new String(byteArrCGI)
//    } else {
//      CGI = ""
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("MmeUeS1apId", (false, null))._1) {
      val lenMmeUeS1apId = dataInput.readInt()
      val byteArrMmeUeS1apId = new Array[Byte](lenMmeUeS1apId)
      for (i <- 0 until lenMmeUeS1apId) {
        byteArrMmeUeS1apId(i) = dataInput.readByte()
      }
      MmeUeS1apId = BigInt(byteArrMmeUeS1apId)
//    } else {
//      MmeUeS1apId = BigInt(0)
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("MmeGroupId", (false, null))._1) {
      MmeGroupId = dataInput.readInt
//    } else {
//      MmeGroupId = 0
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("MmeCode", (false, null))._1) {
      MmeCode = dataInput.readInt
//    } else {
//      MmeCode = 0
//    }
//    if (FieldsMask.allFieldsMask.getOrElse("TimeStamp", (false, null))._1) {
      val lenTimeStamp = dataInput.readInt()
      val byteArrTimeStamp = new Array[Byte](lenTimeStamp)
      for (i <- 0 until lenTimeStamp) {
        byteArrTimeStamp(i) = dataInput.readByte()
      }
      TimeStamp = new String(byteArrTimeStamp)
//    } else {
//      TimeStamp = ""
//    }

    // mrofields: mro_sc, mro_nc, mro_2nd, mro_3nd
    val fieldsArr: Array[AnyVal] = new Array[AnyVal](MroObjectFieldsSeq_int.realFieldsLen)

    var fieldsInd: Int = 0
    for( i <- 7 until FieldsMask.allFields.length) {
      val (isOut, cls) = FieldsMask.allFieldsMask().getOrElse(FieldsMask.allFields(i), (false, null))
      if (isOut) {
        try {
          cls.toUpperCase match {
            case "INT" => fieldsArr(fieldsInd) = dataInput.readInt()
            case "SHORT" => fieldsArr(fieldsInd) = dataInput.readShort()
            case "BYTE" => fieldsArr(fieldsInd) = dataInput.readByte()
            case _ =>
              throw new Exception("反序列化时，日志字段信息的JSON文件描述有错误，问题字段是" + FieldsMask.allFields(i))
//              System.err.println(sm.format(new Date()) +
//                " ERROR MroObjectFieldsSeq: 反序列化时，日志字段信息的JSON文件描述有错误，问题字段是" + FieldsMask.allFields(i))
          }
          fieldsInd = fieldsInd + 1
        } catch {
          case iobe: java.lang.IndexOutOfBoundsException =>
            val e = new Exception("反序列化过程中，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错数据的MroKey是：\n"
              + String.join(",", ENBID.toString, ECI.toString, CGI,
              MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
            e.setStackTrace(iobe.getStackTrace)
            throw e
//            System.err.print(sm.format(new Date()) +
//              " ERROR MroObjectFieldsSeq: 反序列化过程中，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错数据的MroKey是：")
//            System.err.println(
//              String.join(",", ENBID.toString, ECI.toString, CGI,
//                MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
//            iobe.printStackTrace(System.err)
//            System.exit(1)
          case ioe: java.io.IOException =>
            val e = new Exception("反序列化数据时遇到错误。出错数据的MroKey是：\n"
              + String.join(",", ENBID.toString, ECI.toString, CGI,
              MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
            e.setStackTrace(ioe.getStackTrace)
            throw e
//            System.err.print(sm.format(new Date()) +
//              " ERROR MroObjectFieldsSeq: 反序列化数据时遇到错误。出错数据的MroKey是：")
//            System.err.print(
//              String.join(",", ENBID.toString, ECI.toString, CGI,
//                MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
//            System.err.println("，出错的字段是：" + FieldsMask.allFields(i) + "，字段值是：" + fields(i))
//            ioe.printStackTrace(System.err)
//            System.exit(1)
          case ex: Exception =>
            val e = new Exception(String.join("\n",
              "反序列化数据时遇到错误。出错数据的MroKey是：",
              String.join(",", ENBID.toString, ECI.toString, CGI,
                MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp),
              "出错的字段是：" + FieldsMask.allFields(i) + "，字段值是：" + fields(i),
              ex.getMessage))
            e.setStackTrace(ex.getStackTrace)
            throw e
        }
      }
    }

    if ( fieldsInd != MroObjectFieldsSeq_int.realFieldsLen ){
      throw new Exception("序列化数据时，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错的数据MroKey是：\n"
        + String.join(",", ENBID.toString, ECI.toString, CGI,
        MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
//      System.err.println(sm.format(new Date()) +
//        " ERROR MroObjectFieldsSeq: 序列化数据时，实际数据的字段数与日志字段信息JSON文件中的字段数目不一致。出错的数据MroKey是：")
//      System.err.println(
//        String.join(",", ENBID.toString, ECI.toString, CGI,
//          MmeUeS1apId.toString(), MmeGroupId.toString, MmeCode.toString, TimeStamp))
//      System.exit(1)
    }

    this.fields = fieldsArr

  }

  override def toString: String = {
    val builder = new StringBuilder()

    builder.append(ENBID)
    builder.append(",")
    builder.append(ECI)
    builder.append(",")
    builder.append(CGI)
    builder.append(",")
    builder.append(MmeUeS1apId)
    builder.append(",")
    builder.append(MmeGroupId)
    builder.append(",")
    builder.append(MmeCode)
    builder.append(",")
    builder.append(TimeStamp)
    builder.append(",")
    for( i <- 0 until fields.length ){
      builder.append(fields(i)+",")
    }
    builder.deleteCharAt(builder.length-1)

    builder.toString()
  }
}

object MroObjectFieldsSeq_int{
  def apply(ENBID: Int, ECI: Int, CGI: String, MmeUeS1apId: BigInt,
            MmeGroupId: Int, MmeCode: Int, TimeStamp: String, fields: Array[AnyVal]
           ): MroObjectFieldsSeq_int = new MroObjectFieldsSeq_int(
    ENBID, ECI, CGI, MmeUeS1apId, MmeGroupId, MmeCode, TimeStamp, fields
  )

  // MroFields 的字段个数
  val realFieldsLen: Int = try {
    FieldsMask.allFields.count(p => FieldsMask.allFieldsMask().getOrElse(p, (false, null))._1) - 7
  } catch {
    case ex: Exception =>
      val e = new Exception(
        String.join("\n",
          "获取 MroFields 的字段个数 失败",
          ex.getMessage + {
            if (ex.getCause == null) "" else "\n" + ex.getCause.toString
          }
        )
      )
      e.setStackTrace(ex.getStackTrace)
      throw e
      0
  }
}