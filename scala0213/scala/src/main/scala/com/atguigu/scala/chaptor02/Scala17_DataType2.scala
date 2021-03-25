package com.atguigu.scala.chaptor02

/**
 * 数据类型及转换
 * @author zxjgreat
 * @create 2020-05-19 19:40
 */
object Scala17_DataType2 {

  def main(args: Array[String]): Unit = {

    //Null
    //scala将null作为一个特殊的对象进行处理，类型就是Null
    val v = null
    //AnyVal类型不能使用null赋值
    //val i : int = null

    //Nothing, 没有值
    //val nil: List[Nothing] = Nil

    def test() : Nothing = {
        throw new Exception
    }

    val a : Any = "123"
    val r : AnyRef = "123"
    //val v : AnyVal = "123"


    //自动类型转换
    val b : Byte = 10
    val s : Short = b
    val z : Int = s

    //强制类型转换
    val l : Short = z.toShort
    println(l)

  }

}
