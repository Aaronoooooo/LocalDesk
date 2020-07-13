package com.javadesignpattern.template.game;

/**
 * 模板设计模式
 * @author pengfeisu
 * @create 2020-01-14 18:55
 */
public class TemplatePatternDemo {
    public static void main(String[] args) {

        Game game = new Cricket();
        game.play();
        System.out.println();
        game = new Football();
        game.play();
    }
}
