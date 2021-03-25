package com.mobikok.dev.framework.bean


class Task {
  var data : Int = 0
  var logic : Int => Int  = null

  def compute() = {
    logic(data)
  }
}
