package exersize

/**
 *
 * 3. 如果函数调用：  test(10,20,c)的计算结果由参数c来决定，这个参数c你会如何声明
 * @author zxjgreat
 * @create 2020-05-21 11:59
 */
object Test03 {

  def main(args: Array[String]): Unit = {


    def fun(x: Int, y: Int, c: (Int , Int)=>Any)  = {
      c (x , y )
    }



    //val result = fun(10,20,(x :Int,y :Int)=>{ x + y })
//    val result = fun(10,20,(x :Int,y :Int)=> x + y )
//    val result = fun(10,20,(x ,y)=>x + y)
    val result = fun( 10 , 20 ,_ + _ )
    println(result)

  }

}
