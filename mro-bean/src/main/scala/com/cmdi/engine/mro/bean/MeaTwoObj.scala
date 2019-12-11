package com.cmdi.engine.mro.bean

import scala.collection.mutable.ListBuffer

class MeaTwoObj {

  var LteScPlrULQci1:Int = -111
  var LteScPlrULQci2:Int = -111
  var LteScPlrULQci3:Int = -111
  var LteScPlrULQci4:Int = -111
  var LteScPlrULQci5:Int = -111
  var LteScPlrULQci6:Int = -111
  var LteScPlrULQci7:Int = -111
  var LteScPlrULQci8:Int = -111
  var LteScPlrULQci9:Int = -111
  var LteScPlrDLQci1:Int = -111
  var LteScPlrDLQci2:Int = -111
  var LteScPlrDLQci3:Int = -111
  var LteScPlrDLQci4:Int = -111
  var LteScPlrDLQci5:Int = -111
  var LteScPlrDLQci6:Int = -111
  var LteScPlrDLQci7:Int = -111
  var LteScPlrDLQci8:Int = -111
  var LteScPlrDLQci9:Int = -111


  def clean():this.type = {
    LteScPlrULQci1 = -111
    LteScPlrULQci2 = -111
    LteScPlrULQci3 = -111
    LteScPlrULQci4 = -111
    LteScPlrULQci5 = -111
    LteScPlrULQci6 = -111
    LteScPlrULQci7 = -111
    LteScPlrULQci8 = -111
    LteScPlrULQci9 = -111
    LteScPlrDLQci1 = -111
    LteScPlrDLQci2 = -111
    LteScPlrDLQci3 = -111
    LteScPlrDLQci4 = -111
    LteScPlrDLQci5 = -111
    LteScPlrDLQci6 = -111
    LteScPlrDLQci7 = -111
    LteScPlrDLQci8 = -111
    LteScPlrDLQci9 = -111
    this
  }

  def copy()={
    MeaTwoObj(this.LteScPlrULQci1, this.LteScPlrULQci2, this.LteScPlrULQci3, this.LteScPlrULQci4,
      this.LteScPlrULQci5, this.LteScPlrULQci6, this.LteScPlrULQci7, this.LteScPlrULQci8,
      this.LteScPlrULQci9, this.LteScPlrDLQci1, this.LteScPlrDLQci2, this.LteScPlrDLQci3,
      this.LteScPlrDLQci4, this.LteScPlrDLQci5, this.LteScPlrDLQci6, this.LteScPlrDLQci7,
      this.LteScPlrDLQci8, this.LteScPlrDLQci9)
  }


  override def toString = {

    val builder = new StringBuilder()

    builder.append(LteScPlrULQci1)
    builder.append(",")
    builder.append(LteScPlrULQci2)
    builder.append(",")
    builder.append(LteScPlrULQci3)
    builder.append(",")
    builder.append(LteScPlrULQci4)
    builder.append(",")
    builder.append(LteScPlrULQci5)
    builder.append(",")
    builder.append(LteScPlrULQci6)
    builder.append(",")
    builder.append(LteScPlrULQci7)
    builder.append(",")
    builder.append(LteScPlrULQci8)
    builder.append(",")
    builder.append(LteScPlrULQci9)
    builder.append(",")
    builder.append(LteScPlrDLQci1)
    builder.append(",")
    builder.append(LteScPlrDLQci2)
    builder.append(",")
    builder.append(LteScPlrDLQci3)
    builder.append(",")
    builder.append(LteScPlrDLQci4)
    builder.append(",")
    builder.append(LteScPlrDLQci5)
    builder.append(",")
    builder.append(LteScPlrDLQci6)
    builder.append(",")
    builder.append(LteScPlrDLQci7)
    builder.append(",")
    builder.append(LteScPlrDLQci8)
    builder.append(",")
    builder.append(LteScPlrDLQci9)
    builder.append(",")

    builder.toString()
  }

  /**
    * filterList已经是经过筛选并且按照输出顺序进行排列的List
    */
  def getInfo(filterList: java.util.List[String]):String={
    val builder = new StringBuilder()
    import scala.collection.JavaConversions._
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
      case "LteScPlrULQci1"  => LteScPlrULQci1
      case "LteScPlrULQci2"  => LteScPlrULQci2
      case "LteScPlrULQci3"  => LteScPlrULQci3
      case "LteScPlrULQci4"  => LteScPlrULQci4
      case "LteScPlrULQci5"  => LteScPlrULQci5
      case "LteScPlrULQci6"  => LteScPlrULQci6
      case "LteScPlrULQci7"  => LteScPlrULQci7
      case "LteScPlrULQci8"  => LteScPlrULQci8
      case "LteScPlrULQci9"  => LteScPlrULQci9
      case "LteScPlrDLQci1"  => LteScPlrDLQci1
      case "LteScPlrDLQci2"  => LteScPlrDLQci2
      case "LteScPlrDLQci3"  => LteScPlrDLQci3
      case "LteScPlrDLQci4"  => LteScPlrDLQci4
      case "LteScPlrDLQci5"  => LteScPlrDLQci5
      case "LteScPlrDLQci6"  => LteScPlrDLQci6
      case "LteScPlrDLQci7"  => LteScPlrDLQci7
      case "LteScPlrDLQci8"  => LteScPlrDLQci8
      case "LteScPlrDLQci9"  => LteScPlrDLQci9
      case _ =>
    }
  }

}

object MeaTwoObj{

  def apply(LteScPlrULQci1:Int, LteScPlrULQci2:Int, LteScPlrULQci3:Int, LteScPlrULQci4:Int,
            LteScPlrULQci5:Int, LteScPlrULQci6:Int, LteScPlrULQci7:Int, LteScPlrULQci8:Int,
            LteScPlrULQci9:Int, LteScPlrDLQci1:Int, LteScPlrDLQci2:Int, LteScPlrDLQci3:Int,
            LteScPlrDLQci4:Int, LteScPlrDLQci5:Int, LteScPlrDLQci6:Int, LteScPlrDLQci7:Int,
            LteScPlrDLQci8:Int, LteScPlrDLQci9:Int): MeaTwoObj = {

    val obj = new MeaTwoObj()
    obj.LteScPlrULQci1 = LteScPlrULQci1
    obj.LteScPlrULQci2 = LteScPlrULQci2
    obj.LteScPlrULQci3 = LteScPlrULQci3
    obj.LteScPlrULQci4 = LteScPlrULQci4
    obj.LteScPlrULQci5 = LteScPlrULQci5
    obj.LteScPlrULQci6 = LteScPlrULQci6
    obj.LteScPlrULQci7 = LteScPlrULQci7
    obj.LteScPlrULQci8 = LteScPlrULQci8
    obj.LteScPlrULQci9 = LteScPlrULQci9
    obj.LteScPlrDLQci1 = LteScPlrDLQci1
    obj.LteScPlrDLQci2 = LteScPlrDLQci2
    obj.LteScPlrDLQci3 = LteScPlrDLQci3
    obj.LteScPlrDLQci4 = LteScPlrDLQci4
    obj.LteScPlrDLQci5 = LteScPlrDLQci5
    obj.LteScPlrDLQci6 = LteScPlrDLQci6
    obj.LteScPlrDLQci7 = LteScPlrDLQci7
    obj.LteScPlrDLQci8 = LteScPlrDLQci8
    obj.LteScPlrDLQci9 = LteScPlrDLQci9
    obj
  }


  def apply(): MeaTwoObj = new MeaTwoObj()


}
