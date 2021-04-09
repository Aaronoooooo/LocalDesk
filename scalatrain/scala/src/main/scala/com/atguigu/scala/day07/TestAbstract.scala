package com.atguigu.scala.day07

/**
 * * @create 2020-05-26 10:45
 */
object TestAbstract {

  def main(args: Array[String]): Unit = {

    println(new subTest2)


  }

}
//抽象类

abstract class Test{

  val age : Int = 20
  def fun(): Unit
  println(age)

}
//子类继承父类后要要加override关键字
class subTest extends Test{
  val subTest = new subTest
  subTest.age
  override def fun(): Unit = {

  }
}

class subTest2 extends Test{
  //抽象属性要用val修饰，继承后要加override关键字
  override val age: Int = 30
  override def fun(): Unit = {

  }
}
