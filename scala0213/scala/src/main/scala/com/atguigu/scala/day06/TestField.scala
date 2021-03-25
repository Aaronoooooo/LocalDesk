package com.atguigu.scala.day06

import scala.beans.BeanProperty

/**
 * 面向对象-属性
 *
 * @author zxjgreat
 * @create 2020-05-25 18:32
 */
object TestField {

  def main(args: Array[String]): Unit = {

    val user = new User()
//    user.name = "lisi"

//    user.name_$eq("zhangsan")
//    println(user.name)
    //scala类中声明类的属性，不仅仅只有属性，还有其他内容
    //1.编译时，类中的属性其实都是private,并同时提供了公共方法进行访问
    //2.编译生成的方法为类似java中的get/set方法
    //    类似于java中bean对象的get方法，用于返回属性的值
    //    public String name() { return this.name }
    //    类似于java中bean对象的set方法, 用于设定属性的值
    //    public void name_$eq(final String x$1) {this.name = x$1;}
    // 3. 在使用时属性
    //    因为属性为私有的，所以给属性赋值，其实等同于调用属性的set方法
    //    因为属性为私有的，所以获取属性值，其实等同于调用属性的get方法
    // 4. 使用val声明的属性。
    //    在编译时，属性会被直接使用final修饰
    //    在使用的过程中，也不提供属性的set方法。


    // Scala编译生成属性的set，get方法并没有遵循bean规范，这样在很多框架中无法使用。
    // 如果想要scala中的属性符合bean规范，可以采用特殊的注解：@BeanProperty
    user.setName("zxj")
    println(user.getName)
  }
  //内部类
  class User{

    //属性
    //使用下划线对属性进行默认初始化
    @BeanProperty
    var name : String = _
    var age : Int = _
    var sex : String = _
  }
}
