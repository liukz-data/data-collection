package com.cmdi.engine.mro.bean

class MeaThreeObj {

  var LteScRIP2:Int = -111
  var LteScRIP7:Int = -111

  def clean():this.type = {
    LteScRIP2 = -111
    LteScRIP7 = -111
    this
  }

  def copy() = {
    MeaThreeObj(this.LteScRIP2, this.LteScRIP7)
  }
  override def toString = LteScRIP2 + "," + LteScRIP7

  /**
    * filterList已经是经过筛选并且按照输出顺序进行排列的List
    */
  def getInfo(filterList: java.util.List[String]): String = {
    import scala.collection.JavaConversions._
    val builder = new StringBuilder()
    for (name <- filterList) {
      if(name.equals("LteScRIP2")){
        builder.append(LteScRIP2)
        builder.append(",")
      } else if (name.equals("LteScRIP7")) {
        builder.append(LteScRIP7)
        builder.append(",")
      }
    }
    val str = builder.toString()
    if(str.length > 0){
      str.substring(0,str.length-1)
    } else {
      ""
    }
  }
}

object MeaThreeObj{

  def apply(LteScRIP2:Int,LteScRIP7:Int ): MeaThreeObj = {
    val obj = new MeaThreeObj

    obj.LteScRIP2 = LteScRIP2
    obj.LteScRIP7 = LteScRIP7

    obj
  }

  def apply(): MeaThreeObj = new MeaThreeObj()

}
