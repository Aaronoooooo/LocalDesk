package com.javareverse;

public class Reverse {
    public static void main(String[] args) {
        String str = "abcd";
        int in = 123456;
        int reverseIn = Integer.reverse(in);
        StringBuffer sb = new StringBuffer(str);
        String reverseStr = sb.reverse().toString();
        System.out.println("reverseIn: " + reverseIn + "\nreverseStr: " + reverseStr);
    }
}
