package com.atguigu.scala.day07

/**
 *
 * 单例模式
 * * @create 2020-05-26 18:38
 */
object TestSingleton {

  def main(args: Array[String]): Unit = {

    val user1 = User01
    val user2 = User01

    println(user1 eq user2)
  }


}

class User01 private{

}
object User01{
  val user01 = new User01

  def apply(): User01 = {
    user01
  }
}