package com.atguigu.scala.day03

/**
 * 函数至简原则
 * * @create 2020-05-20 19:52
 */
object TestFuntion2 {

  def main(args: Array[String]): Unit = {

    //1.省略return
    //当函数需要返回值时，可以将函数体中最后一行执行的代码作为返回结果
    //所以可以省略return关键字
//    def fun1(): String ={
//      "lili"
//    }
    //2.如果编译器可以推断出函数的返回值类型，那么返回值类型可以省略
//    def fun2() ={
//      "name"
//    }
    //3.如果函数体逻辑只有一行代码，那么大括号可以省略
//    def fun3() = "name"
    //4.如果函数没有提供参数，那么调用的时候，小括号可以省略
    //如果函数没有提供参数,那么声明时，小括号也可以省略, 调用时，必须不能使用小括号
//    def fun4() = "zhangsan"

//    println(fun4())
//    def fun5 = "lisi"
//
//    println(fun5)

    //5.函数如果明确使用Unit声明没有返回值，那么函数体中的return关键字不起作用的。
    //函数体中如果明确使用return关键字，那么返回值类型不能省
    //明确函数就是没有返回值，但是Unit又不想声明,那么可以同时省略等号
    //将这种函数的声明方式称之为过程函数, 不省略花括号

//    def fun6(): Unit ={
//      return "zhangsan"
//    }
//      def fun6(){return "zhangsan"}
    // 6. 当只关心代码逻辑，不关心函数名称时，函数名和def关键字可以省略
    //将没有名称和def关键字的函数称之为匿名函数
    //匿名函数规则： （参数列表） => { 代码逻辑 }
    //函数调用 ： 函数（变量）名称（参数列表）
    // 声明匿名函数

    val a = () => println("no name")

    //调用
    a()



  }

}
