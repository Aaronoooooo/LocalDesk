package com.atguigu.scala.day08

import scala.io.Source

/**
 * * @create 2020-05-27 20:38
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    // Scala - WordCount
    // 需求：将文件中单词统计出现的次数并排序取前三名
//    //1.从文件获取一行一行的数据
//    val dataList: List[String] = Source.fromFile("input/word.txt").getLines().toList
//    //2.将一行一行的字符串拆分成单词
//    val wordList: List[String] = dataList.flatMap(data=>{data.split(" ")})
//    //3.根据单词进行分组：相同的单词放置在一个组中
//    val wordGroupMap: Map[String, List[String]] = wordList.groupBy(word=>word)
//    //4.将分组后的数据进行次数统计
//    val wordCountMap: Map[String, Int] = wordGroupMap.map(kv=>{(kv._1,kv._2.length)})
//    //5.将统计结果的次数按降序排序
//    val sortByList: List[(String, Int)] = wordCountMap.toList.sortBy(kv=>kv._2)(Ordering.Int.reverse)
//    //6.将排序后的结果取前三名
//   val result: List[(String, Int)] = sortByList.take(3)
//    println(result)
//    //1.从文件中获取数据，一行一行的字符串
//    val dataList: List[String] = Source.fromFile("input/word.txt").getLines().toList
//    //2.将一行一行的字符串转换成单词
//    val wordList: List[String] = dataList.flatMap(data=>{data.split(" ")})
//    //3.将单词分组
//    val groupByMap: Map[String, List[String]] = wordList.groupBy(word=>word)
//    //4.统计相同单词的个数
//    val wordCountMap: Map[String, Int] = groupByMap.map(kv=>{(kv._1,kv._2.length)})
//    //5.排序
//    val sortByList: List[(String, Int)] = wordCountMap.toList.sortBy(kv=>kv._2)(Ordering.Int.reverse)
//    //6.打印前三名
//    println(sortByList.take(3))
    //1.从文件中获取数据，数据为一行一行的字符串
//    val dataList: List[String] = Source.fromFile("input/word.txt").getLines().toList
//    //2.将字符转转换为单词
//    val wordList: List[String] = dataList.flatMap(data=>data.split(" "))
//    //3.将单词分组
//    val groupMap: Map[String, List[String]] = wordList.groupBy(word=>word)
//    //4.计算单词的个数
//    val wordCountMap: Map[String, Int] = groupMap.map(kv=>{(kv._1,kv._2.length)})
//    //5.排序
//    val sortList: List[(String, Int)] = wordCountMap.toList.sortBy(kv=>kv._2)(Ordering.Int.reverse)
//    //6.取前三
//    println(sortList.take(3))
    //1.从文件中获取数据
    val dataList: List[String] = Source.fromFile("input/word.txt").getLines().toList
    //2.将数据转换为单词
    val wordList: List[String] = dataList.flatMap(data=>data.split(" "))
    //3.将单词分组
    val groupMap: Map[String, List[String]] = wordList.groupBy(word=>word)
    //4.求出相同单词的个数
    val wordCountMap: Map[String, Int] = groupMap.map(kv=>(kv._1,kv._2.length))
    //5.排序
    val sortList: List[(String, Int)] = wordCountMap.toList.sortBy(kv=>kv._2)(Ordering.Int.reverse)
    //6.取前三
    val result: List[(String, Int)] = sortList.take(3)
    println(result)

  }

}
