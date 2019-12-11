package com.output

import java.io.{BufferedReader, InputStreamReader}
import java.{lang, util}

import com.cmdi.engine.mro.bean.MroKey

/**
  * 本类用于 从解析的csv.gz文件中 把每一行数据变为 HashMap[MroKey, Mrofields]中的一个条目
  * @param in 本地调试用数据文件的输入流
  */
class GetMroObjFromFile_main(in: InputStreamReader) {
  def getMroObj: util.HashMap[MroKey, util.List[Object]] = {
    val mroObj: util.HashMap[MroKey, util.List[Object]] = new util.HashMap[MroKey, util.List[Object]]
    val br: BufferedReader = new BufferedReader(in)
    var mrofieldsName: Array[String] = null

    try {
      var str: String = null
      var firstLine = true
//      var cnt = 0
      str = br.readLine
      while (str!=null) {
        if (firstLine){
          mrofieldsName = str.split(",")
          firstLine = false
        } else {
          val fileds: Array[String] = str.split(",")
          val mroKey: MroKey = getMroKey(fileds)
          val mrofields: util.List[Object] = getMroFields(fileds, mrofieldsName)
          mroObj.put(mroKey, mrofields)
//          cnt = cnt + 1
//          println(cnt)
        }

        str = br.readLine
      }
      return mroObj
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (br != null) br.close()
    null
  }

  private def getMroKey(fields: Array[String]): MroKey = {
    val mroKey = MroKey.apply(
      fields(0).toInt,
      fields(1).toInt,
      fields(2),
      BigInt.apply(fields(3)),
      fields(4).toInt,
      fields(5).toInt,
      fields(6)
    )

    mroKey
  }

  private def getMroFields(fields: Array[String], mrofieldsName: Array[String]): util.List[Object] = {
    val mrofields: util.ArrayList[Object] = new util.ArrayList[Object](fields.length - 7)
    val patLteNcEarfcn="(LteNcEarfcn[0-9]{1,2})".r
    val patLteNcPci="(LteNcPci[0-9]{1,2})".r
    val patLteNcRSRP="(LteNcRSRP[0-9]{1,2})".r
    val patLteNcRSRQ="(LteNcRSRQ[0-9]{1,2})".r
    val patTdsNcellUarfcn="(TdsNcellUarfcn[0-9]{1,2})".r
    val patTdsCellParameterId="(TdsCellParameterId[0-9]{1,2})".r
    val patTdsPccpchRSCP="(TdsPccpchRSCP[0-9]{1,2})".r
    val patGsmNcellNcc="(GsmNcellNcc[0-9]{1,2})".r
    val patGsmNcellBcc="(GsmNcellBcc[0-9]{1,2})".r
    val patGsmNcellBcch="(GsmNcellBcch[0-9]{1,2})".r
    val patGsmNcellCarrierRSSI="(GsmNcellCarrierRSSI[0-9]{1,2})".r
    val patLteScPlrULQci="(LteScPlrULQci[0-9]{1,2})".r
    val patLteScPlrDLQci="(LteScPlrDLQci[0-9]{1,2})".r
println("fileds length:"+fields.length)
    for( i <- 7 until fields.length ){

      mrofieldsName(i) match {
        case  "LteScRSRP" | "LteScRSRQ" | "LteScPHR" | "LteScSinrUL" |
              "LteScPUSCHPRBNum" | "LteScPDSCHPRBNum"  | "LteScBSR"  | "LteSceNBRxTxTimeDiff" =>
          mrofields.add(i-7, new lang.Byte(fields(i)))
        case patLteNcRSRP(_) | patLteNcRSRQ(_) | patTdsCellParameterId(_) |
            patTdsPccpchRSCP(_) | patGsmNcellNcc(_) | patGsmNcellBcc(_) |
            patGsmNcellCarrierRSSI(_) =>
          mrofields.add(i-7, new java.lang.Byte(fields(i)))
        case "LteScPci" | "LteScTadv" | "LteScAOA" | "LteScRI1" |
             "LteScRI2" | "LteScRI4" | "LteScRI8" | "LteScRIP2" | "LteScRIP7"=>
          mrofields.add(i-7, new java.lang.Short(fields(i)))
        case patLteNcPci(_) | patTdsNcellUarfcn(_) | patGsmNcellBcch(_) |
             patLteScPlrULQci(_) | patLteScPlrDLQci(_) =>
          mrofields.add(i-7, new java.lang.Short(fields(i)))
        case "LteScEarfcn" | patLteNcEarfcn(_) =>
          println("mrofieldsName:"+mrofieldsName(i))
          println("fields(i)):"+fields(i))
          mrofields.add(i-7, new java.lang.Integer(fields(i)))
      }
//      mrofields.add(i-7, fields(i))
    }

    mrofields
  }
}
