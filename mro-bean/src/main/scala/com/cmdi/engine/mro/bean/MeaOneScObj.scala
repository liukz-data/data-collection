package com.cmdi.engine.mro.bean

class MeaOneScObj {

  var LteScEarfcn: Int = -111
  var LteScPci: Int = -111
  var LteScRSRP: Int = -111
  var LteSceNBRxTxTimeDiff: Int = -111
  var LteScTadv: Int = -111
  var LteScRSRQ: Int = -111
  var LteScPUSCHPRBNum: Int = -111
  var LteScPHR: Int = -111
  var LteScAOA: Int = -111
  var LteScSinrUL: Int = -111
  var LteScRI1: Int = -111
  var LteScRI2: Int = -111
  var LteScRI4: Int = -111
  var LteScRI8: Int = -111
  var LteScBSR: Int = -111
  var LteScPDSCHPRBNum: Int = -111

  def clean(): this.type = {
    LteScEarfcn = -111
    LteScPci = -111
    LteScRSRP = -111
    LteSceNBRxTxTimeDiff = -111
    LteScTadv = -111
    LteScRSRQ = -111
    LteScPUSCHPRBNum = -111
    LteScPHR = -111
    LteScAOA = -111
    LteScSinrUL = -111
    LteScRI1 = -111
    LteScRI2 = -111
    LteScRI4 = -111
    LteScRI8 = -111
    LteScBSR = -111
    LteScPDSCHPRBNum = -111
    this
  }

  def copy() = {

    MeaOneScObj(this.LteScEarfcn, this.LteScPci, this.LteScRSRP, this.LteSceNBRxTxTimeDiff,
      this.LteScTadv, this.LteScRSRQ, this.LteScPUSCHPRBNum, this.LteScPHR,
      this.LteScAOA, this.LteScSinrUL, this.LteScRI1, this.LteScRI2, this.LteScRI4,
      this.LteScRI8, this.LteScBSR, this.LteScPDSCHPRBNum)

  }


  override def toString = {

    val builder = new StringBuilder()

    builder.append(LteScEarfcn)
    builder.append(",")
    builder.append(LteScPci)
    builder.append(",")
    builder.append(LteScRSRP)
    builder.append(",")
    builder.append(LteSceNBRxTxTimeDiff)
    builder.append(",")
    builder.append(LteScTadv)
    builder.append(",")
    builder.append(LteScRSRQ)
    builder.append(",")
    builder.append(LteScPUSCHPRBNum)
    builder.append(",")
    builder.append(LteScPHR)
    builder.append(",")
    builder.append(LteScAOA)
    builder.append(",")
    builder.append(LteScSinrUL)
    builder.append(",")
    builder.append(LteScRI1)
    builder.append(",")
    builder.append(LteScRI2)
    builder.append(",")
    builder.append(LteScRI4)
    builder.append(",")
    builder.append(LteScRI8)
    builder.append(",")
    builder.append(LteScBSR)
    builder.append(",")
    builder.append(LteScPDSCHPRBNum)
    builder.append(",")

    builder.toString()
  }

  /**
    * filterList已经是经过筛选并且按照输出顺序进行排列的List
    */
//  def getInfo(filterList: ListBuffer[String]): String = {
//    val builder = new StringBuilder()
//    for (name <- filterList) {
//      val value = switchInfo(name)
//      builder.append(value)
//      builder.append(",")
//    }
//
//    builder.toString()
//  }


  def getInfo(filterList: java.util.List[String]): String = {
    val builder = new StringBuilder()
    import scala.collection.JavaConversions._
    for (name <- filterList) {
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
  private def switchInfo(info: String): Any = {
    info match {
      case "LteScEarfcn" => LteScEarfcn
      case "LteScPci" => LteScPci
      case "LteScRSRP" => LteScRSRP
      case "LteSceNBRxTxTimeDiff" => LteSceNBRxTxTimeDiff
      case "LteScTadv" => LteScTadv
      case "LteScRSRQ" => LteScRSRQ
      case "LteScPUSCHPRBNum" => LteScPUSCHPRBNum
      case "LteScPHR" => LteScPHR
      case "LteScAOA" => LteScAOA
      case "LteScSinrUL" => LteScSinrUL
      case "LteScRI1" => LteScRI1
      case "LteScRI2" => LteScRI2
      case "LteScRI4" => LteScRI4
      case "LteScRI8" => LteScRI8
      case "LteScBSR" => LteScBSR
      case "LteScPDSCHPRBNum" => LteScPDSCHPRBNum
      case _ =>
    }
  }

}


object MeaOneScObj {

  def apply(LteScEarfcn: Int, LteScPci: Int, LteScRSRP: Int, LteSceNBRxTxTimeDiff: Int,
            LteScTadv: Int, LteScRSRQ: Int, LteScPUSCHPRBNum: Int, LteScPHR: Int,
            LteScAOA: Int, LteScSinrUL: Int, LteScRI1: Int, LteScRI2: Int, LteScRI4: Int,
            LteScRI8: Int, LteScBSR: Int, LteScPDSCHPRBNum: Int): MeaOneScObj = {

    val obj = new MeaOneScObj()

    obj.LteScEarfcn = LteScEarfcn
    obj.LteScPci = LteScPci
    obj.LteScRSRP = LteScRSRP
    obj.LteSceNBRxTxTimeDiff = LteSceNBRxTxTimeDiff
    obj.LteScTadv = LteScTadv
    obj.LteScRSRQ = LteScRSRQ
    obj.LteScPUSCHPRBNum = LteScPUSCHPRBNum
    obj.LteScPHR = LteScPHR
    obj.LteScAOA = LteScAOA
    obj.LteScSinrUL = LteScSinrUL
    obj.LteScRI1 = LteScRI1
    obj.LteScRI2 = LteScRI2
    obj.LteScRI4 = LteScRI4
    obj.LteScRI8 = LteScRI8
    obj.LteScBSR = LteScBSR
    obj.LteScPDSCHPRBNum = LteScPDSCHPRBNum
    obj
  }

  def apply(): MeaOneScObj = {
    val mfs = new MeaOneScObj
    mfs
  }


}
