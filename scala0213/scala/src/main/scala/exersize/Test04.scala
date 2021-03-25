package exersize

/**
 * @author zxjgreat
 * @create 2020-05-22 18:06
 */
object Test04 {

  def main(args: Array[String]): Unit = {

    def filter(s : String , fun: (String)=>Boolean): String = {

      val words: Array[String] = s.split(" ")
      var result = ""
      for (word <- words){
        if (fun(word)){
          result += ", " + word
        }
      }

      result.substring(2)

    }

    //filter("hello world scala spark", (word)=>{word.startsWith("s")})
    println(filter("hello world scala spark", _.startsWith("s")))
  }

}
