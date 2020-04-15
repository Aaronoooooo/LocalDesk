package com.javadesign;

/**
 * 策略设计模式
 * @author pengfeisu
 * @create 2020-01-14 19:10
 */
public class StrategyPatternDemo {
    public static void main(String[] args) {
        Context context = new Context();
        context.setStrategy(new OperationAdd());
        System.out.println(" + " + context.executeStrategy(1,2));
        /*Context context = new Context(new OperationAdd());
        System.out.println("10 + 5 = " + context.executeStrategy(10, 5));

        context = new Context(new OperationSubstract());
        System.out.println("10 - 5 = " + context.executeStrategy(10, 5));

        context = new Context(new OperationMultiply());
        System.out.println("10 * 5 = " + context.executeStrategy(10, 5));*/
    }
}
