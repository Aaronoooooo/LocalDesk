package com.atguigu.summer.framework.bean

/**
 * @author zxjgreat
 * @create 2020-06-08 22:24
 */
class Task {
  var data : Int = 0
  var logic : Int => Int  = null

  def compute() = {
    logic(data)
  }
}
