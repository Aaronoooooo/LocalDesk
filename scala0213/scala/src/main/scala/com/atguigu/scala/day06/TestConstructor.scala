package com.atguigu.scala.day06

/**
 * @author zxjgreat
 * @create 2020-05-25 16:12
 */
object TestConstructor {

  def main(args: Array[String]): Unit = {
    val user1 = new User
    val user2 = new User("zhangsan")
    val user3 = new User("zhangsan",26)
    println(user1)
    println(user2)
    println(user3)



  }

}
class User(){
  println("*****")
  def this(name:String) ={
    this()
  }
  def this(name:String,age:Int)={
    this()
  }
}
