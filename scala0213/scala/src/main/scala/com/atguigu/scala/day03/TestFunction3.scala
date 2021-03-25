package com.atguigu.scala.day03

/**
 * 函数作为变量
 * @author zxjgreat
 * @create 2020-05-20 21:15
 */
object TestFunction3 {
  def main(args: Array[String]): Unit = {

    def test1() ={
      "zhangsan"
    }

    def test11(i : Int): Int = {
      i * 10
    }

    // 将函数赋值给变量，那么这个变量其实就是函数，可以调用
    // 函数如果没有参数列表，那么调用时可以省略小括号
    // 如果此时希望将函数不执行，而是当成一个整体赋值给变量，那么需要使用下划线
//    val f1 = test1 _
//    println(f1())

    // 如果不想使用下划线明确将函数作为整体使用，那么也可以直接声明变量的类型为函数
    // 函数类型 ： 参数列表 => 返回值类型
    val f1 : () => String = test1
    println(f1())

    val ff1 : (Int)=>Int = test11
    println(ff1(10))
  }

}
