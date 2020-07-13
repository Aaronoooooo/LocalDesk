package com.javareflect.interfaces;

import java.io.Serializable;

/**
 *
 * @create 2019 下午 3:12
 */
public class Creature<T> implements Serializable {
    private char gender;
    public double weight;

    private void breath(){
        System.out.println("生物呼吸");
    }

    public void eat(){
        System.out.println("生物吃东西");
    }

}
