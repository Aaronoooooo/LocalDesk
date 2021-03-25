package com.atguigu.scala.day08

/**
 * @author zxjgreat
 * @create 2020-05-27 18:36
 */
object TestCollection_Array {

  def main(args: Array[String]): Unit = {
    //foreach循环遍历
    val array = Array(1,2,3,4,5)
    def fun(i : Int) ={
      println(i)
    }

//    array.foreach(fun)
//    array.foreach((i:Int)=>{println(i)})
    array.foreach(println(_))
    array.foreach(println)









  }

}
