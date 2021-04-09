package com.atguigu.scala.chaptor02

/**
 * * @create 2020-05-19 18:20
 */
object Scala02_Variable {
  def main(args: Array[String]): Unit = {


    //变量声明
    var i : Int = 20

    // 如果可以通过语法推断出来变量类型，那么变量在声明时可以省略类型
    // Scala是静态类型语言，在编译时必须要确定类型的

    var name = "zhangxiujun"

    //如果一行代码中有两个逻辑，则需要加;一个时省略
     println(i);println(name)

    //在scala中使用val声明变量，表示变量不可被修改
    val username = "pyy"

    //变量必须显示初始化
    val user : String = "lisi"
  }

}
