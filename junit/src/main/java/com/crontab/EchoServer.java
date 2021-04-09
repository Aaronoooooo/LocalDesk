package com.crontab;

/**
 * 创建多线程之实现Runnable接口
 */
public class EchoServer implements Runnable {
    @Override
    public void run() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("This is a echo server. The current time is " +
                System.currentTimeMillis() + ".");
    }
}
