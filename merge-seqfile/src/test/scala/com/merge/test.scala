package com.merge
import util.control.Breaks._

object test {

  def main(args: Array[String]): Unit = {

    val arr = Array(1,2,3,4,6,5)
    for(a <- arr){

      breakable{
        if(a==2){
          println(222)
          break
        }

      }
      println(a)
    }

  }
}
