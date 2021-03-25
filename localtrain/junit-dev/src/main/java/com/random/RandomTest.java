package com.random;

public class RandomTest {
    public static void main(String[] args) {
        double random = Math.random();
        for (int i = 1; i <= 10; i++) {
            System.out.print((int)(Math.random() * (99-10) + 10) + "\t");
        //    System.out.println(new Random().nextInt(10));
        //    System.out.println((int) random);
        //    System.out.println("************************");
        }
        System.out.println();
        double min = 0.01;
        double max = 10;
        int scl = 2;
        int pow = (int)Math.pow(10,scl);
        double one = Math.floor((Math.random() * (max - min) + min) * pow) / pow;
        for (int i = 0; i < 10; i++) {
            System.out.print(one + "\t");
        }
    }


}
