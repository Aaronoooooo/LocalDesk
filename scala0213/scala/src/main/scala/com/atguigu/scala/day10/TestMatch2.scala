package com.atguigu.scala.day10

/**
 * @author zxjgreat
 * @create 2020-05-31 9:25
 */
object TestMatch2 {
  def main(args: Array[String]): Unit = {

    // TODO 样例类
    // 使用case关键字声明的类, 称之为样例类
    // 专门用于匹配对象
    // 1. 样例类在编译时，会自动生成伴生对象以及apply方法
    // 2. 样例类的构造参数默认使用val声明，所以参数其实就是类的属性
    //    如果想要更改属性，需要显示的将属性使用var声明
    // 3. 样例类会自动生成unapply方法
    // 4. 样例类自动实现可序列化接口
    // 在实际开发中，一般都使用样例类，便于开发。
    val emp = Emp( "lisi",20 )
    emp match {
      case Emp("lisi",30) => println("xxxxx")
      case _ => println("yyyy")
    }



    // TODO 匹配对象

    // Scala中模式匹配对象时，会自动调用对象的unapply方法进行匹配
    // 这里的匹配对象，其实匹配的是对象的属性是否相同

    //val user = new User("zhangsan", 20);
    val user = User("zhangsan", 20)

    val result = user match {
      case User("zhangsan", 20) => "yes"
      case _ => "no"
    }

    //println(result)

  }
  // 声明样例类
  case class Emp( var name: String, age: Int )
  // 声明伴生类
  class User(val name: String, val age: Int)
  // 声明伴生对象
  object User {
    // 使用参数自动构建对象
    def apply(name: String, age: Int): User = new User(name, age)
    // 使用过对象自动获取参数
    def unapply(user: User): Option[(String, Int)] = {
      Option( ( user.name, user.age ) )
    }
  }

}
