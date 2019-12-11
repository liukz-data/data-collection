package com.cmdi.engine.mro.bean

import java.util



class MroKey {
  var ENBID = -111
  var ECI = -111
  var CGI : String= "Nil"
  var MmeUeS1apId: BigInt = BigInt(-1)
  var MmeGroupId: Int = -111
  var MmeCode: Int = -111
  var TimeStamp: String = "Nil"

  /** 只用作标记measurement3的2和7 */
  var tmpNumber = -1

  def getTmpnumber() = {
    this.tmpNumber
  }

  def setNumber(num: Int) = {
    this.tmpNumber = num
  }


  def copy(): MroKey = {
    val copyed = MroKey(this.ENBID, this.ECI, this.CGI, this.MmeUeS1apId, this.MmeGroupId, this.MmeCode, this.TimeStamp)
    copyed
  }

  def clear: this.type = {
    ECI = -111
    CGI = "Nil"
    MmeUeS1apId = BigInt(-111);
    MmeGroupId = -111
    MmeCode = -111
    TimeStamp = "Nil"
    tmpNumber = -1
    this
  }

  override def toString = {

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

    builder.toString()
  }

  /**
    * filterList已经是经过筛选并且按照输出顺序进行排列的List
    */
  def getInfo(filterList: util.List[String]):String={
    import scala.collection.JavaConversions._
    val builder = new StringBuilder()
    for (name <- filterList){
      val value = switchInfo(name)
      builder.append(value)
      builder.append(",")
    }
    builder.toString()
  }

  private def switchInfo(info:String): Any  ={
    info match {
      case "ENBID" => ENBID
      case "ECI" => ECI
      case "CGI" =>  CGI
      case "MmeUeS1apId" => MmeUeS1apId
      case "MmeGroupId" => MmeGroupId
      case "MmeCode" => MmeCode
      case "TimeStamp" => TimeStamp
      case _ =>
    }
  }


  def canEqual(a: Any) = a.isInstanceOf[MroKey]

  override def equals(that: Any): Boolean =
    that match {
      case that: MroKey => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + ECI + MmeCode + MmeGroupId + MmeUeS1apId.toInt
    result = prime * result + (if (TimeStamp == null) 0 else TimeStamp.hashCode)
    return result
  }
}

object MroKey {
  def apply(ENBID: Int, ECI:Int, CGI:String,  MmeUeS1apId: BigInt, MmeGroupId: Int,
            MmeCode: Int, TimeStamp: String): MroKey = {
    val mk = new MroKey
    mk.ENBID = ENBID;
    mk.ECI = ECI
    mk.CGI = CGI
    mk.MmeUeS1apId = MmeUeS1apId
    mk.MmeGroupId = MmeGroupId;
    mk.MmeCode = MmeCode
    mk.TimeStamp = TimeStamp
    mk
  }

  def apply(): MroKey = {
    val mk = new MroKey
    mk
  }

}

class Mrofields {

  var meaOneScObj: MeaOneScObj = MeaOneScObj()
  //创建长度为12的不可变数组存储MeaOneNcObj
  var meaOneNcObjArray: Array[MeaOneNcObj] = Array(MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj(), MeaOneNcObj())
  var meaTwoObj: MeaTwoObj = MeaTwoObj()
  var meaThreeObj: MeaThreeObj = MeaThreeObj()

  def copy(): Mrofields = {

    val meaSc = this.meaOneScObj.copy()

    import scala.collection.mutable.ArrayBuffer

    val meaOneNcArray = new ArrayBuffer[MeaOneNcObj]()

    for(obj <- this.meaOneNcObjArray){
      meaOneNcArray += obj.copy()
    }

    val meaTwo = this.meaTwoObj.copy()
    val meaThree = this.meaThreeObj.copy()

    val copyed = Mrofields(meaSc, meaOneNcArray.toArray, meaTwo, meaThree)
    copyed
  }

  def clear: this.type = {

    meaOneScObj.clean()
    for(obj <- meaOneNcObjArray){
      obj.clean
    }
    meaTwoObj.clean()
    meaThreeObj.clean()
    this
  }

  //此处顺序要和util中的顺序对应
  override def toString: String = {

    val builder = new StringBuilder()

    builder.append(this.meaOneScObj.toString)

    for(obj <- this.meaOneNcObjArray){
      builder.append(obj.toString)
    }

    builder.append(this.meaTwoObj.toString)

    builder.append(this.meaThreeObj.toString)

    builder.toString()
  }


  //此处顺序要和util中的顺序对应
  def getInfo(scList: util.List[String], ncList: util.List[String], twoList: util.List[String], threeList: util.List[String],
              ncNum:Int): String = {

    val builder = new StringBuilder()

    builder.append(this.meaOneScObj.getInfo(scList))

    for( index <- 0 until ncNum){
      val obj: MeaOneNcObj = this.meaOneNcObjArray(index)
      builder.append(obj.getInfo(ncList))
    }

    builder.append(this.meaTwoObj.getInfo(twoList))

    builder.append(this.meaThreeObj.getInfo(threeList))

    builder.toString()
  }

}

object Mrofields {
  def apply(meaOneScObj: MeaOneScObj, meaOneNcObjArray: Array[MeaOneNcObj], meaTwoObj: MeaTwoObj, meaThreeObj: MeaThreeObj): Mrofields = {
    val mfs = new Mrofields

    mfs.meaOneScObj = meaOneScObj
    mfs.meaOneNcObjArray = meaOneNcObjArray
    mfs.meaTwoObj = meaTwoObj
    mfs.meaThreeObj = meaThreeObj
    mfs
  }

  def apply(): Mrofields = {
    val mfs = new Mrofields
    mfs
  }
}

object MeasurementNumber extends Enumeration {
  type MeasurementNumber = Value
  val NotYet, FirstBlock, SecondBlock, ThirdBlock = Value
}
