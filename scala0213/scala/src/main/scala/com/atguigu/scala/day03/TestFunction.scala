package com.atguigu.scala.day03

/**
 * @author zxjgreat
 * @create 2020-05-20 19:02
 */
object TestFunction {

  def main(args: Array[String]): Unit = {
    //无参，无返回值
    def fun1(): Unit ={
      println("fun1...")
    }
    fun1()
    //无参，有返回值
    def fun2(): String ={
      return "zxj"
    }

    println(fun2())
    //有参，无返回值
    def fun3(i : Int): Unit ={
      println(i)
    }
    fun3(26)
    //有参，有返回值
    def fun4(name : String): String ={
      return name
    }

    println(fun4("pyy"))
    //多参，无返回值
    def fun5(name : String, age : Int): Unit ={
      println("name = " + name , "age = " + age )
    }
    fun5("zxj", 26)
    //多参，有返回值
    def fun6(name : String, hello : String): String = {
       name + " " + hello
    }

    println(fun6("zxj", "hello"))
  }
}
