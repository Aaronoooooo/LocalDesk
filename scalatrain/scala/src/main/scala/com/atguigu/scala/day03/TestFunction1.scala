package com.atguigu.scala.day03

/**
 * * @create 2020-05-20 19:22
 */
object TestFunction1 {

  def main(args: Array[String]): Unit = {

    //可变形参,只能放在参数列表的最后
    def fun(names : String*): Unit ={
      println(names)
    }
    fun("zhangsan","lisi","wangwu")


    //参数默认值
    def fun1(name : String , password : String= "000000"): Unit ={
      println("name = "+ name + "," + "password = " + password)
    }
    fun1("zhang" , "123456")
    fun1("zxj")

    //带名参数
    def fun2(name : String , password : String = "000000" , tel :String = "12345678910"): Unit ={
      println("name = "+ name + "," + "password = " + password + "," + "tel" + tel)
    }
    fun2("pyy", tel = "12142354345")
  }

}
