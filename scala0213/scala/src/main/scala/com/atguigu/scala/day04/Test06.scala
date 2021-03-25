package com.atguigu.scala.day04

/**
 *
 * 控制抽象
 * @author zxjgreat
 * @create 2020-05-22 20:56
 */
object Test06 {

  def main(args: Array[String]): Unit = {
//    def fun : String ={
//      "zhangsan"
//    }
//    test(fun)
//    def test(f : =>String): Unit ={
//      println(f)
//    }

    def fun : String = {
    "nihaoya"
    }

    test(fun)
    def test(f : =>String): Unit ={
      println(f)
    }

  }

}
