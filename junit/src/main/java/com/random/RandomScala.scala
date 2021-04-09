package com.random

object RandomScala {
  def main(args: Array[String]): Unit = {
    for(i <- 1 to 10){
      val int = (Math.random() * (99 - 10) + 10).toInt
      print(int+ "\t")
    }
    println()
    print((Math.random() * (99 - 10) + 10).toInt+ "\t")
  }

}
