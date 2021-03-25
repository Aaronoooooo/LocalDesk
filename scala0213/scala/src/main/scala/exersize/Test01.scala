package exersize

/**
 *
 * 1. 如果想把一个任意的数字A通过转换后得到它的2倍，那么这个转换的函数应该如何声明和使用
 *
 * @author zxjgreat
 * @create 2020-05-21 10:13
 */
object Test01 {

  def main(args: Array[String]): Unit = {

    def mult(i: Double) = {
      i * 2
    }

    val f = mult _
    println(f(3.14))

//    val user = new User
//    user.name = "zhangsan"
//    println("name = " + user.name)

//  }



}
//class User{
//  var name : String = ""

}
