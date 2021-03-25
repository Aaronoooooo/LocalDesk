package com.atguigu.scala.day07

/**
 * 特质
 * @author zxjgreat
 * @create 2020-05-26 19:09
 */
object TestInterface {

  def main(args: Array[String]): Unit = {

    //Scala中没有接口的概念,也就没有interface关键字。
    // Scala可以将多个类中相同的特征从类里剥离出来，形成特殊的语法结构，称之为“特质”
    // 如果一个类，符合这个特征，那么就可以将这个特征（特质）混入到类中,使用extends关键字

    val oper: Operate = new Test03

    oper.fun()
  }

}
trait Operate{
  def fun(): Unit
}

class Test03 extends Operate {
  override def fun(): Unit = {
    println("test...")
  }
}


