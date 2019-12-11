package com.hyyx

import java.util

import com.cmdi.engine.mro.bean.MroKey
import com.mro.schema.{FieldsMask, MroObjectFieldsSeq}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.{IOUtils, NullWritable, SequenceFile}

import scala.collection.JavaConversions._

object SeqFileOutputtest {
  /**
    * 将MRO Map数据转化成 SequenceFile 输出到 HDFS上
    * @param mroObj MRO Map实例数据
    * @param hdfsPath SequenceFile输出到HDFS上的路径
    * @return 0 表示正常输出完毕，1 表示输出过程中遇到错误或异常
    *
    */
  def outputAsSequenceFile(mroObj: util.HashMap[MroKey, util.List[Object]],
                           hdfsPath: String, hdfsConf: Configuration): Int = {
    var succ = 0

    // 0. 先判断 日志字段配置信息的json文件 文件是否能被正确解析，不能则直接返回到主程序
//    if (FieldsMask.allFieldsMask==null){
//      exMsg = "日志字段配置信息json文件的内容没有被正确解析，SequenceFile输出程序退出！"
//      return 1
//    }
    try {
      FieldsMask.allFieldsMask() == null
    } catch {
      case ex: Exception =>
        exMsg = "日志字段配置信息json文件的内容没有被正确解析，SequenceFile输出程序退出！"+
              ex.getMessage + "\n    " + ex.getStackTrace.mkString("\n    ")
        return 1
    }

    // 1. 根据 java.util.HashMap[MroKey, Mrofields] MroObj实例构造 MroObjectFieldsSeq 实例
    //val MroObjectFieldsSeqArr = transferToWritable(mroObj)

    // 2. 构造 SequenceFile.Writer实例，并将 MroObj实例中的数据写入SequenceFile
    var writerSeqFile: SequenceFile.Writer = null

    try{
     /* writerSeqFile = SequenceFile.createWriter(hdfsConf,
        SequenceFile.Writer.file(new Path(hdfsPath)),
        SequenceFile.Writer.keyClass(classOf[NullWritable]),
        SequenceFile.Writer.valueClass(classOf[MroObjectFieldsSeq]),
        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec))*/
      writerSeqFile = createWriter(hdfsPath,hdfsConf)
      //创建序列化对象并写入
      transferToWritable(mroObj,writerSeqFile)
      //MroObjectFieldsSeqArr.foreach( writerSeqFile.append(NullWritable.get(), _) )

    } catch {
      case ex: Exception =>
        exMsg = String.join("\n",
          "写入SequenceFile时发生错误，SequenceFile输出程序退出！",
          ex.getMessage + { if (ex.getCause==null) "" else "\n"+ex.getCause.toString },
          ex.getStackTrace.mkString("\n"))
        succ = 1
    } finally {
      IOUtils.closeStream(writerSeqFile)
    }
    succ
  }

 // private def transferToWritable(mroObj: util.HashMap[MroKey, util.List[Object]],writerSeqFile: SequenceFile.Writer): Array[MroObjectFieldsSeq] = {
    private def transferToWritable(mroObj: util.HashMap[MroKey, util.List[Object]],writerSeqFile: SequenceFile.Writer): Unit = {
      val len = mroObj.size()
    //val MroObjectFieldsSeqArr = new Array[MroObjectFieldsSeq](len)

    var i = 0
    val keySet = mroObj.keySet()
    val it1 = keySet.iterator()

    while(it1.hasNext){
      val k = it1.next()
      val arrLst = mroObj.get(k)
      val fields = new Array[AnyVal](arrLst.size())
      for( i<- arrLst.indices) {
        fields(i) = arrLst.get(i).asInstanceOf[AnyVal]
      }
     /* MroObjectFieldsSeqArr(i) = MroObjectFieldsSeq.apply(
        k.ENBID, k.ECI, k.CGI, k.MmeUeS1apId, k.MmeGroupId, k.MmeCode, k.TimeStamp, fields
      )*/
      val mroObjectFieldsSeqmOut = MroObjectFieldsSeq.apply(
        k.ENBID, k.ECI, k.CGI, k.MmeUeS1apId, k.MmeGroupId, k.MmeCode, k.TimeStamp, fields
      )
      writerSeqFile.append(NullWritable.get(),mroObjectFieldsSeqmOut)
//      val meaOneScObj = mroObj.get(k).meaOneScObj
//      val meaOneNcObjArray = mroObj.get(k).meaOneNcObjArray
//      val meaTwoObj = mroObj.get(k).meaTwoObj
//      val meaThreeObj = mroObj.get(k).meaThreeObj
//      MroObjectFieldsSeqArr(i) = MroObjectFieldsSeq.apply(
//        k.ENBID, k.ECI, k.CGI, k.MmeUeS1apId, k.MmeGroupId, k.MmeCode, k.TimeStamp,
//        Array(meaOneScObj.LteScEarfcn, meaOneScObj.LteScPci, meaOneScObj.LteScRSRP,
//        meaOneScObj.LteSceNBRxTxTimeDiff, meaOneScObj.LteScTadv, meaOneScObj.LteScRSRQ,
//        meaOneScObj.LteScPUSCHPRBNum, meaOneScObj.LteScPHR, meaOneScObj.LteScAOA, meaOneScObj.LteScSinrUL,
//        meaOneScObj.LteScRI1, meaOneScObj.LteScRI2, meaOneScObj.LteScRI4, meaOneScObj.LteScRI8,
//        meaOneScObj.LteScBSR, meaOneScObj.LteScPDSCHPRBNum,
//        meaOneNcObjArray(0).LteNcEarfcn, meaOneNcObjArray(0).LteNcPci, meaOneNcObjArray(0).LteNcRSRP,
//        meaOneNcObjArray(0).LteNcRSRQ, meaOneNcObjArray(0).TdsNcellUarfcn, meaOneNcObjArray(0).TdsCellParameterId,
//        meaOneNcObjArray(0).TdsPccpchRSCP, meaOneNcObjArray(0).GsmNcellNcc, meaOneNcObjArray(0).GsmNcellBcc,
//        meaOneNcObjArray(0).GsmNcellBcch, meaOneNcObjArray(0).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(1).LteNcEarfcn, meaOneNcObjArray(1).LteNcPci, meaOneNcObjArray(1).LteNcRSRP,
//        meaOneNcObjArray(1).LteNcRSRQ, meaOneNcObjArray(1).TdsNcellUarfcn, meaOneNcObjArray(1).TdsCellParameterId,
//        meaOneNcObjArray(1).TdsPccpchRSCP, meaOneNcObjArray(1).GsmNcellNcc, meaOneNcObjArray(1).GsmNcellBcc,
//        meaOneNcObjArray(1).GsmNcellBcch, meaOneNcObjArray(1).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(2).LteNcEarfcn, meaOneNcObjArray(2).LteNcPci, meaOneNcObjArray(2).LteNcRSRP,
//        meaOneNcObjArray(2).LteNcRSRQ, meaOneNcObjArray(2).TdsNcellUarfcn, meaOneNcObjArray(2).TdsCellParameterId,
//        meaOneNcObjArray(2).TdsPccpchRSCP, meaOneNcObjArray(2).GsmNcellNcc, meaOneNcObjArray(2).GsmNcellBcc,
//        meaOneNcObjArray(2).GsmNcellBcch, meaOneNcObjArray(2).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(3).LteNcEarfcn, meaOneNcObjArray(3).LteNcPci, meaOneNcObjArray(3).LteNcRSRP,
//        meaOneNcObjArray(3).LteNcRSRQ, meaOneNcObjArray(3).TdsNcellUarfcn, meaOneNcObjArray(3).TdsCellParameterId,
//        meaOneNcObjArray(3).TdsPccpchRSCP, meaOneNcObjArray(3).GsmNcellNcc, meaOneNcObjArray(3).GsmNcellBcc,
//        meaOneNcObjArray(3).GsmNcellBcch, meaOneNcObjArray(3).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(4).LteNcEarfcn, meaOneNcObjArray(4).LteNcPci, meaOneNcObjArray(4).LteNcRSRP,
//        meaOneNcObjArray(4).LteNcRSRQ, meaOneNcObjArray(4).TdsNcellUarfcn, meaOneNcObjArray(4).TdsCellParameterId,
//        meaOneNcObjArray(4).TdsPccpchRSCP, meaOneNcObjArray(4).GsmNcellNcc, meaOneNcObjArray(4).GsmNcellBcc,
//        meaOneNcObjArray(4).GsmNcellBcch, meaOneNcObjArray(4).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(5).LteNcEarfcn, meaOneNcObjArray(5).LteNcPci, meaOneNcObjArray(5).LteNcRSRP,
//        meaOneNcObjArray(5).LteNcRSRQ, meaOneNcObjArray(5).TdsNcellUarfcn, meaOneNcObjArray(5).TdsCellParameterId,
//        meaOneNcObjArray(5).TdsPccpchRSCP, meaOneNcObjArray(5).GsmNcellNcc, meaOneNcObjArray(5).GsmNcellBcc,
//        meaOneNcObjArray(5).GsmNcellBcch, meaOneNcObjArray(5).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(6).LteNcEarfcn, meaOneNcObjArray(6).LteNcPci, meaOneNcObjArray(6).LteNcRSRP,
//        meaOneNcObjArray(6).LteNcRSRQ, meaOneNcObjArray(6).TdsNcellUarfcn, meaOneNcObjArray(6).TdsCellParameterId,
//        meaOneNcObjArray(6).TdsPccpchRSCP, meaOneNcObjArray(6).GsmNcellNcc, meaOneNcObjArray(6).GsmNcellBcc,
//        meaOneNcObjArray(6).GsmNcellBcch, meaOneNcObjArray(6).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(7).LteNcEarfcn, meaOneNcObjArray(7).LteNcPci, meaOneNcObjArray(7).LteNcRSRP,
//        meaOneNcObjArray(7).LteNcRSRQ, meaOneNcObjArray(7).TdsNcellUarfcn, meaOneNcObjArray(7).TdsCellParameterId,
//        meaOneNcObjArray(7).TdsPccpchRSCP, meaOneNcObjArray(7).GsmNcellNcc, meaOneNcObjArray(7).GsmNcellBcc,
//        meaOneNcObjArray(7).GsmNcellBcch, meaOneNcObjArray(7).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(8).LteNcEarfcn, meaOneNcObjArray(8).LteNcPci, meaOneNcObjArray(8).LteNcRSRP,
//        meaOneNcObjArray(8).LteNcRSRQ, meaOneNcObjArray(8).TdsNcellUarfcn, meaOneNcObjArray(8).TdsCellParameterId,
//        meaOneNcObjArray(8).TdsPccpchRSCP, meaOneNcObjArray(8).GsmNcellNcc, meaOneNcObjArray(8).GsmNcellBcc,
//        meaOneNcObjArray(8).GsmNcellBcch, meaOneNcObjArray(8).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(9).LteNcEarfcn, meaOneNcObjArray(9).LteNcPci, meaOneNcObjArray(9).LteNcRSRP,
//        meaOneNcObjArray(9).LteNcRSRQ, meaOneNcObjArray(9).TdsNcellUarfcn, meaOneNcObjArray(9).TdsCellParameterId,
//        meaOneNcObjArray(9).TdsPccpchRSCP, meaOneNcObjArray(9).GsmNcellNcc, meaOneNcObjArray(9).GsmNcellBcc,
//        meaOneNcObjArray(9).GsmNcellBcch, meaOneNcObjArray(9).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(10).LteNcEarfcn, meaOneNcObjArray(10).LteNcPci, meaOneNcObjArray(10).LteNcRSRP,
//        meaOneNcObjArray(10).LteNcRSRQ, meaOneNcObjArray(10).TdsNcellUarfcn, meaOneNcObjArray(10).TdsCellParameterId,
//        meaOneNcObjArray(10).TdsPccpchRSCP, meaOneNcObjArray(10).GsmNcellNcc, meaOneNcObjArray(10).GsmNcellBcc,
//        meaOneNcObjArray(10).GsmNcellBcch, meaOneNcObjArray(10).GsmNcellCarrierRSSI,
//        meaOneNcObjArray(11).LteNcEarfcn, meaOneNcObjArray(11).LteNcPci, meaOneNcObjArray(11).LteNcRSRP,
//        meaOneNcObjArray(11).LteNcRSRQ, meaOneNcObjArray(11).TdsNcellUarfcn, meaOneNcObjArray(11).TdsCellParameterId,
//        meaOneNcObjArray(11).TdsPccpchRSCP, meaOneNcObjArray(11).GsmNcellNcc, meaOneNcObjArray(11).GsmNcellBcc,
//        meaOneNcObjArray(11).GsmNcellBcch, meaOneNcObjArray(11).GsmNcellCarrierRSSI,
//        meaTwoObj.LteScPlrULQci1, meaTwoObj.LteScPlrULQci2, meaTwoObj.LteScPlrULQci3, meaTwoObj.LteScPlrULQci4, meaTwoObj.LteScPlrULQci5,
//        meaTwoObj.LteScPlrULQci6, meaTwoObj.LteScPlrULQci7, meaTwoObj.LteScPlrULQci8, meaTwoObj.LteScPlrULQci9,
//        meaTwoObj.LteScPlrDLQci1, meaTwoObj.LteScPlrDLQci2, meaTwoObj.LteScPlrDLQci3, meaTwoObj.LteScPlrDLQci4, meaTwoObj.LteScPlrDLQci5,
//        meaTwoObj.LteScPlrDLQci6, meaTwoObj.LteScPlrDLQci7, meaTwoObj.LteScPlrDLQci8, meaTwoObj.LteScPlrDLQci9,
//        meaThreeObj.LteScRIP2, meaThreeObj.LteScRIP7)
//      )
      i = i + 1
    }
    //新增逻辑

    //MroObjectFieldsSeqArr
  }

  private var exMsg: String = _

  def getExceptionMessage: String = exMsg

  private def createWriter(hdfsPath: String, hdfsConf: Configuration):SequenceFile.Writer={
    //ew Snappy

    val writerSeqFile = SequenceFile.createWriter(hdfsConf,
      SequenceFile.Writer.file(new Path(hdfsPath)),
      SequenceFile.Writer.keyClass(classOf[NullWritable]),
      SequenceFile.Writer.valueClass(classOf[MroObjectFieldsSeq]),
         SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec))
    // SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new SnappyCodec))
    writerSeqFile
  }



}
