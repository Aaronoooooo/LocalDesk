package com.scala.scalalist

/**
  * @author pengfeisu
  * @create 2020-07-22 9:37
  */
object Test {
  def main(args: Array[String]) {
    val site1 = "Runoob" :: ("Google" :: ("Baidu" :: Nil))
    val site2 = "Facebook" :: ("Taobao" :: Nil)
    val fruit = site1 ::: site2
    println(fruit)
    val nums = Nil
    println( "第一个网站是:" + site2.head )
    println( "最后一个网站是:" + site1.tail )
    println( "查看nums是否为空:" + nums.isEmpty )
  }
}
