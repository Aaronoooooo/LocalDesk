package com.atguigu.scala.day04

/**
 * 函数柯里化
 *
 * * @create 2020-05-22 15:20
 */
object Test02 {

  def main(args: Array[String]): Unit = {

//    def test(i: Int)(j: Int) = {
//      i * j
//
//    }
//
//    println(test(10)(20))

   def test(i : Int)(j : Int)(f:(Int,Int)=>Int): Int ={
     f ( i , j )
   }

    println(test(10)(20)(_ + _))
  }

}
