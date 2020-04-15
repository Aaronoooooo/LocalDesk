package com.javadesign;

/**
 * @author pengfeisu
 * @create 2020-01-14 19:09
 */
public class Context {


    private Strategy strategy;

    public Context() {
    }

    public Context(Strategy strategy) {
        this.strategy = strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public int executeStrategy(int num1, int num2) {
        return strategy.doOperation(num1, num2);
    }
}
