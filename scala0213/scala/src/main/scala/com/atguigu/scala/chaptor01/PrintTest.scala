package com.atguigu.scala.chaptor01

/**
 * @author zxjgreat
 * @create 2020-05-18 21:04
 */
object PrintTest {

  def main(args: Array[String]): Unit = {

    val age = 20
    val name = "zhang"
    //字符串连接
//    println("name: " + name + " , " + " age: " + age)
    //传值字符串
//    printf("name = %s , age = %s",name,age)

    //插值字符串
//    println(s"name = $name" , s"age = $age")

    //多行字符串
    println(

      s"""
        | {"name = $name , age = $age"}
        | """.stripMargin
    )

  }

}
