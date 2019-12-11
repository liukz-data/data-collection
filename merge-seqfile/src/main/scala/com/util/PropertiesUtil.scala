package com.util

import java.io.FileInputStream
import java.util.Properties

object PropertiesUtil {

  def getProperties(proFilePath:String):Properties = {
   val in  = new FileInputStream(proFilePath)
   val pro = new Properties()
    pro.load(in)
   pro
  }
}
