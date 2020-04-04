package com.work.util

import java.util.Random

/**
  * @author star 
  */
object RandomNum {
  def apply(fromNum:Int,toNum:Int): Int = {
    fromNum+new Random().nextInt(toNum-fromNum+1)
  }

  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean): String ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
//    用delimiter分割  canRepeat为false则不允许重复
//      此处逻辑请同学们自行实现
//    "1,2,3"
    val random = new Random()
    var builder = new StringBuilder

    for (i <- 1 to amount){
      var tmp = random.nextInt(toNum-fromNum+1)+fromNum
      while (!canRepeat && builder.toString().contains(tmp+"")){
        tmp = random.nextInt(toNum-fromNum+1)+fromNum
      }
      builder.append(tmp).append(",")
    }
    val i: Int = builder.lastIndexOf(delimiter)
    builder.delete(i,i+1).toString()
  }

  def main(args: Array[String]): Unit = {
    println(multi(1, 10, 3, ",", false))
  }
}
