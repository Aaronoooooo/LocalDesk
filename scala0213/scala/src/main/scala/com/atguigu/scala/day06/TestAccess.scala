package com.atguigu.scala.day06

/**
 * 访问权限
 * @author zxjgreat
 * @create 2020-05-25 20:56
 */
object TestAccess {

  def main(args: Array[String]): Unit = {

/*

   Java : 4种访问权限

   private   : 私有权限，  同类
   (default) : 包权限，    同类，同包
   protected : 受保护权限，同类，同包，子类
   public    : 公共权限，  任意的地方


   Scala : 4种访问权限
   private            : 私有权限，  同类
   private[包名]      : 包权限，    同类，同包
   protected          : 受保护权限，同类，同包，子类
    (default)         : 公共权限，  任意的地方。scala中没有public关键字

 */

    //private
    val user = new User()

    user.test()

}
  class User{
    private var name : String = "zhangsan"
    private[scala] var age : Int = 20
    protected var tel : String = "1232434"
    var email:String = "xxxx@xxx.com"
    def test(): Unit ={
      println("age = " + age)
      println("name = " + name)
    }
  }

  class SubUser extends User {
    def testSub(): Unit = {
      println(this.tel)
      //println(this.name)
      println(this.age)
      println(this.email)
    }
  }

  class Fun{
    val user = new User()
    user.age
  }

}
