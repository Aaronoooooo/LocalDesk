package com.javadesignpattern.strategy.season.operation;

/**
 * @author pengfeisu
 * @create 2020-04-23 9:44
 */
public class Context {
    private flowStrategy strategy;

    public Context(flowStrategy strategy){
        this.strategy = strategy;
    }

    public int executeStrategy(int num1, int num2){
        return strategy.doOperation(num1, num2);
    }
}