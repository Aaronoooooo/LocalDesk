package com.javadesignpattern.strategy.season.operation;

/**
  * @create 2020-04-23 9:44
 */
public class OperationMultiply implements flowStrategy {
    @Override
    public int doOperation(int num1, int num2) {
        return num1 * num2;
    }
}
