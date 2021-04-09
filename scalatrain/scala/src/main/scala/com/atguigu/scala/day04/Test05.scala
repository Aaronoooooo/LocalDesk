package com.atguigu.scala.day04

/**
 *惰性函数
 * * @create 2020-05-22 19:33
 */
object Test05 {

  def main(args: Array[String]): Unit = {
    def test() ={
      println("test...")
      "nhwjzxj"
    }
    lazy val name = test()
    println("*********************")
    println("name : " + name)
   //println(test())

  }

}
