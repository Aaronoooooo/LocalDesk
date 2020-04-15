package ${packageName}

import java.lang.{Double => Jdouble, Long => Jlong}

class NumberFormatUtil {

    def panduanLong(longNum:String):Long={
        val num:Long =0
        if (longNum.equals("null")){
            num
        }else{
            Jlong.valueOf(longNum)
        }
    }

    def  panduanInt(intNum:String):Int={
        val num:Int =0
        if ("null".equals(intNum)||intNum==null){
            num
        }else{
            Integer.valueOf(intNum)
        }
    }

    def panduanDouble(doubleNum:String):Double= {
        val num:Double = 0.0
        if("null".equals(doubleNum)||doubleNum==null){
            num
        }else{
            Jdouble.valueOf(doubleNum)
        }
    }

    def panduanDate(date:String):String= {
    val num:String = "1970-01-01 00:00:00"
        if("null".equals(date)||date==null){
            num
        }else{
            date
        }
    }


}
