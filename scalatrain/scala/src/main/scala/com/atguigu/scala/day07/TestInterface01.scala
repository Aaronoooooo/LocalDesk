package com.atguigu.scala.day07

/**
 * * @create 2020-05-26 19:19
 */
object TestInterface01 {
  def main(args: Array[String]): Unit = {
    // 动态扩展功能,遵循 OCP 开发原则

    // 1. 特质中不仅仅有抽象方法，还可以有具体方法

    // 2. 如果对象声明后想要扩展功能，怎么办？

    // 3. 特质 (混入with)

    val mysql = new MySQL33() with Operate33
    mysql.insert()
    mysql.select()


  }

}
trait Operate33 {
  def insert(): Unit = {
    println("insert data....")
  }
}

class MySQL33 {
  def select(): Unit = {
    println("select data....")
  }
}
