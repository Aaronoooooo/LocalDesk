package com.javadesign;

/**
 * @author pengfeisu
 * @create 2020-01-14 19:08
 */
public class OperationAdd implements Strategy{
    @Override
    public int doOperation(int num1, int num2) {
        return num1 + num2;
    }
}
