package com.atguigu.scala.day03

/**
 * 函数作为参数
 * @author zxjgreat
 * @create 2020-05-20 21:46
 */
object TestFunction4 {

  def main(args: Array[String]): Unit = {

    //2.函数可以作为参数传递给其他的函数
    // 函数名（参数名1:参数类型1，参数名2，参数类型2）
    // 函数名（ 参数名1 ：函数类型）
//    def test2(f : (Int)=>Int) ={
//      f(10)
//    }
//
//    def fun2(i : Int)={
//      i * 10
//    }
//
//    val f = fun2 _
//    println(test2(f))
    def fun2(i : Int) ={
      i * 10
    }
    def test2(f : (Int)=>Int) ={
      f(10)
    }
    val f = fun2 _
//    val f : (Int)=>Int = fun2

    println(test2(f))

    //2.2将函数作为参数使用时，一般不关心函数的名称,所以一般使用匿名函数
    //匿名函数规则：（参数列表） => { 代码逻辑 }
//    val result = test2((i:Int)  => {i * 10})
//
//    println(result)
    //至简原则
//    val result = test2((i:Int)  => {i * 10})
    //1.如果逻辑中只有一行代码，花括号可以省略
//    val result = test2((i:Int)  => i * 10)
    //如果匿名函数的参数类型可以推断出来，那么类型可以省略
//    val result = test2((i)  => i * 10)
    //如果匿名函数的参数列表只有一个或没有，那么小括号可以省略
//    val result = test2(i  => i * 10)
    //如果匿名函数中的参数在逻辑中只使用了一次，那么，参数和=>可以省略,使用_代替
    val result = test2(_* 10)
    println(result)
  }

}
