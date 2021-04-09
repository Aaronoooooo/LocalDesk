package exersize

/**
 * 1. 如果想把一个任意的数字A通过转换后得到它的2倍，那么这个转换的函数应该如何声明和使用
 * 2. 如果上一题想将数字A转换为任意数据B（不局限为数字），转换规则自己定义，该如何声明
 *
 * * @create 2020-05-21 10:21
 */
object Test02 {
  def main(args: Array[String]): Unit = {

    def fun( i : Int) ={
      val i = 20
      if(i > 20 ){
        i * 2
      }else{
        "love"
      }
    }

    val f = fun _

    println(f(30))
  }

}
