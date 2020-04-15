package com.fwmagic.flink;

public class Demo {
    public static void main(String[] args) throws Exception{
        int retry =3;
        int i =1;
        while(retry-- >0){
            System.out.println("retry = "+retry);
            if(1 ==2){
                System.out.println("循环直接结束！");
                break;
            }
            System.out.println("重试 "+i+" 次");
            i++;
            Thread.sleep(1000);
        }
    }
}
