package com.util

import java.security.{Key, SecureRandom}

import javax.crypto.{Cipher, KeyGenerator}
import sun.misc.{BASE64Decoder, BASE64Encoder}

object DecryptTool {
  private val KEY_STR = "LogDB"
  private val key: Key = {
    try {
      val generator = KeyGenerator.getInstance("DES")
      val secureRandom = SecureRandom.getInstance("SHA1PRNG")
      secureRandom.setSeed(KEY_STR.getBytes)
      generator.init(secureRandom)
      generator.generateKey()
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
      null
    }
  }

//  def main(args: Array[String]): Unit = {
//    println(getEncryptString("#Mrdas17P19!"))
//    println(getDecryptString(getEncryptString("#Mrdas17P19!")))
//  }

  // 对字符串进行加密，返回BASE64的加密字符串
  private def getEncryptString(str: String): String = {
    val base4Encoder = new BASE64Encoder
    try {
      val strBytes = str.getBytes("UTF-8")
      val cipher = Cipher.getInstance("DES")
      cipher.init(Cipher.ENCRYPT_MODE, key)
      val encryptStrBytes = cipher.doFinal(strBytes)
      base4Encoder.encode(encryptStrBytes)
    } catch {
      case ex: Exception =>
        throw new RuntimeException(ex)
        null
    }
  }

  //对BASE64加密字符串进行解密
  def getDecryptString(str: String): String = {
    val base64Decoder = new BASE64Decoder
    try {
      val strBytes = base64Decoder.decodeBuffer(str)
      val cipher = Cipher.getInstance("DES")
      cipher.init(Cipher.DECRYPT_MODE, key)
      val decryptStrBytes = cipher.doFinal(strBytes)
      new String(decryptStrBytes, "UTF-8")
    } catch {
      case ex: Exception =>
        throw new RuntimeException(ex)
        null
    }
  }

}
