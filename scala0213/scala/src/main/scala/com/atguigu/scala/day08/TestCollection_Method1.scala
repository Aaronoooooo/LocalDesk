package com.atguigu.scala.day08

/**
 * @author zxjgreat
 * @create 2020-05-27 20:07
 */
object TestCollection_Method1 {
  def main(args: Array[String]): Unit = {

    // Scala - 集合 - 常用方法 - 操作数据
    //   val list = List(1,2,3,4,5,6)
    //    def transform(i : Int) ={
    //      i * 2
    //    }

    //    val list1: List[Int] = list.map(transform)
    //    val list1: List[Int] = list.map(_*2)
    //    println(list1)
    //扁平化flatmap
//    val list = List(List(1, 2), List(3, 4))
//    def transform(list: List[Int]) ={
//      list.map(_*2)
//    }
//
//    println(list.flatMap(_.map(_ * 2)))

//    val list = List("hello scala", "hello spark")
//    def transform(s:String) ={
//      s.split(" ")
//    }
//
//    println(list.flatMap(_.split(" ")))

    // TODO 过滤
    // 按照指定的规则对集合中的每一条数据进行筛选过滤。
    // 满足条件的数据保留，不满足条件的数据丢弃
    //        val list = List(1,2,3,4)
    //        println(list.filter((num) => {
    //            num % 2 == 0
    //        }))

//            val list = List("hello", "spark", "scala", "hadoop")
//            println(list.filter(word => {
//                word.startsWith("s")
//            }))
    // TODO 排序
    // 将集合中每一个数据按照指定的规则进行排序
    // 默认排序为升序
    // sortBy可以使用函数柯里化实现降序
    //        val list = List(3,1,4,2)
    //        println(list.sortBy(num => num))
    //        println(list.sortBy(num => -num))
    //        println(list.sortBy(num => num).reverse)
    //        println(list.sortBy(num => num)(Ordering.Int.reverse))
    //val list = List("1", "2", "11", "3")

    // 字符串的排序可以按照字典顺序
    //println(list.sortBy(s => s))
    // "1" => 1
    // "2" => 2
    // "11" => 11
    // "3"  => 3
    //println(list.sortBy(s => s.toInt))

    // 自定义类型数据排序
    val user1 = new User()
    user1.age = 20
    user1.name = "zhangsan"

    val user2 = new User()
    user2.name = "lisi"
    user2.age = 20

    val user3 = new User()
    user3.name = "wangwu"
    user3.age = 10

    val list = List(user2, user1, user3 )

    // scala中的元组自动比较大小。
    // 先比较第一个元素，再比较第二个元素，异常类推
    println(list.sortBy(user => {
      (user.age, user.name)
    }))

  }
  class User {
    var name : String = _
    var age : Int = _

    override def toString: String = {
      s"User[$name, $age]"
    }
  }

}
