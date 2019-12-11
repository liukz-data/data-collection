package com.cmdi.engine.mro.bean

import scala.collection.mutable.ListBuffer

class MeaOneNcObj {

  var LteNcEarfcn: Int = -111
  var LteNcPci: Int = -111
  var LteNcRSRP: Int = -111
  var LteNcRSRQ: Int = -111
  var TdsNcellUarfcn: Int = -111
  var TdsCellParameterId: Int = -111
  var TdsPccpchRSCP: Int = -111
  var GsmNcellNcc: Int = -111
  var GsmNcellBcc: Int = -111
  var GsmNcellBcch: Int = -111
  var GsmNcellCarrierRSSI: Int = -111


  def clean: this.type = {
    LteNcEarfcn = -111
    LteNcPci = -111
    LteNcRSRP = -111
    LteNcRSRQ = -111
    TdsNcellUarfcn = -111
    TdsCellParameterId = -111
    TdsPccpchRSCP = -111
    GsmNcellNcc = -111
    GsmNcellBcc = -111
    GsmNcellBcch = -111
    GsmNcellCarrierRSSI = -111
    this
  }


  def copy() = {

    MeaOneNcObj(this.LteNcEarfcn, this.LteNcPci, this.LteNcRSRP, this.LteNcRSRQ,
      this.TdsNcellUarfcn, this.TdsCellParameterId, this.TdsPccpchRSCP, this.GsmNcellNcc,
      this.GsmNcellBcc, this.GsmNcellBcch, this.GsmNcellCarrierRSSI)
  }


  override def toString = {

    val builder = new StringBuilder()

    builder.append(LteNcEarfcn)
    builder.append(",")
    builder.append(LteNcPci)
    builder.append(",")
    builder.append(LteNcRSRP)
    builder.append(",")
    builder.append(LteNcRSRQ)
    builder.append(",")
    builder.append(TdsNcellUarfcn)
    builder.append(",")
    builder.append(TdsCellParameterId)
    builder.append(",")
    builder.append(TdsPccpchRSCP)
    builder.append(",")
    builder.append(GsmNcellNcc)
    builder.append(",")
    builder.append(GsmNcellBcc)
    builder.append(",")
    builder.append(GsmNcellBcch)
    builder.append(",")
    builder.append(GsmNcellCarrierRSSI)
    builder.append(",")

    builder.toString()
  }

  /**
    * filterList已经是经过筛选并且按照输出顺序进行排列的List
    */
  def getInfo(filterList: java.util.List[String]):String={
    import scala.collection.JavaConversions._
    val builder = new StringBuilder()
    for (name <- filterList){
      val value = switchInfo(name)
      builder.append(value)
      builder.append(",")
    }
    builder.toString()
  }

  /**
    * 通过字段名称的对比选择具体的数值
    * @param info 需要选择的字段名称
    * @return 该字段的具体数值
    */
  private def switchInfo(info:String): Any  ={
    info match {
      case "LteNcEarfcn" => LteNcEarfcn
      case "LteNcPci" => LteNcPci
      case "LteNcRSRP" => LteNcRSRP
      case "LteNcRSRQ" => LteNcRSRQ
      case "TdsNcellUarfcn" => TdsNcellUarfcn
      case "TdsCellParameterId" => TdsCellParameterId
      case "TdsPccpchRSCP" => TdsPccpchRSCP
      case "GsmNcellNcc" => GsmNcellNcc
      case "GsmNcellBcc" => GsmNcellBcc
      case "GsmNcellBcch" => GsmNcellBcch
      case "GsmNcellCarrierRSSI" => GsmNcellCarrierRSSI
      case _ =>
    }
  }





}

object MeaOneNcObj {
  def apply(LteNcEarfcn: Int, LteNcPci: Int, LteNcRSRP: Int, LteNcRSRQ: Int,
            TdsNcellUarfcn: Int, TdsCellParameterId: Int, TdsPccpchRSCP: Int, GsmNcellNcc: Int,
            GsmNcellBcc: Int, GsmNcellBcch: Int, GsmNcellCarrierRSSI: Int): MeaOneNcObj = {

    val obj = new MeaOneNcObj()

    obj.LteNcEarfcn = LteNcEarfcn
    obj.LteNcPci = LteNcPci
    obj.LteNcRSRP = LteNcRSRP
    obj.LteNcRSRQ = LteNcRSRQ
    obj.TdsNcellUarfcn = TdsNcellUarfcn
    obj.TdsCellParameterId = TdsCellParameterId
    obj.TdsPccpchRSCP = TdsPccpchRSCP
    obj.GsmNcellNcc = GsmNcellNcc
    obj.GsmNcellBcc = GsmNcellBcc
    obj.GsmNcellBcch = GsmNcellBcch
    obj.GsmNcellCarrierRSSI = GsmNcellCarrierRSSI

    obj
  }

  def apply(): MeaOneNcObj = {
    val mfs = new MeaOneNcObj
    mfs
  }
}
