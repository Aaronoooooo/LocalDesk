package com.atguigu.scala.day03

/**
 * * @create 2020-05-20 22:34
 */
object TestFunction5 {

  def main(args: Array[String]) = {

    def fun(f : (Int,Int)=>Int): Int ={

      val boyCount = 25
      val girlCount = 25

      f(boyCount,girlCount)

    }

    def sum(b : Int , g : Int) ={

      b + g
    }

    //val result = fun((b:Int,g:Int)=>{b + g})
    //至简原则
    //逻辑中只有一行代码，省略花括号
    //val result = fun((b:Int,g:Int)=>b + g)
    //参数类型可以判断，省略参数类型
    //val result = fun((b,g)=>b + g)
    //参数列表中的参数只使用了一次，则使用下划线代替参数和=>
    val result = fun(_ + _)
    println(result)



  }

}
