package com.atguigu.scala.day09

/**
 * @author zxjgreat
 * @create 2020-05-29 17:35
 */
object TestCollection_Method {

  def main(args: Array[String]): Unit = {
    //对多个集合数据进行处理
//    val list1 = List(1, 2, 3, 4)
//    val list2 = List(3, 4, 5, 6, 7)
//    // 并集，合集
//    println(list1.union(list2))
//    //交集
//    println(list1.intersect(list2))
//    //差集
//    //当前集合减去交叉重合的数据
//    println(list1.diff(list2))//1,2
//    println(list2.diff(list1))//5,6,7
    // TODO 两个集合数据进行关联 ： 相同位置的关联
    // 如果两个集合的元素个数不相同，那么拉链后的数据取决于个数少的那个集合
    // TODO 拉链
//    println(list1.zip(list2))
//    // 自关联 : 数据和索引关联
//    val list3 = List("a", "b", "c")
//    println(list3.zipWithIndex)


    // TODO 将数据的一部分作为整体来使用操作的函数
//    val list = List(1,2,3,4,5,6,7,8)
//    val iterator: Iterator[List[Int]] = list.sliding(3,3)
//    while (iterator.hasNext){
//      println(iterator.next())
//    }

//    val list = List(1,2,3,4,5,6)
//
//    // mapReduce
//    // TODO 简化，规约
//    // reduce参数 ： 【op; (A1, A1) => A1】
//    // reduce中传递的参数的规则：参数和返回值类型相同
//    // 这里的参数其实就是集合中数据的类型
//    // scala中集合的计算基本上都是两两计算
//    println(list.reduce(_ + _))
//    println(list.reduceLeft(_ + _))
//    println(list.reduceRight(_ + _))
//    println(list.reduce(_ - _))
//    println(list.reduceLeft(_ - _))
//    println(list.reduceRight(_ - _))

    // sortWith : 用于判断左边数据是否小于右边数据，
    //            如果满足（true），升序，不满足（false）就是降序
    // TODO 实现时，只需要考虑满足我们自定义条件的场合下，返回true就可以。
    val list = List((1,2),(2,3))
    val swList: List[(Int, Int)] = list.sortWith((Left, Right) => {
      if (Left._1 < Right._1) {
        true
      } else if (Left._1 == Right._1) {
        Left._2 > Right._2
      } else {
        false
      }
    })
    println(swList)










  }

}
