package com.fwmagic.flink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TryWithResourceDemo {

    public static void main(String[] args) throws Exception {
//       test();
        testTryWith();
    }

    private static void testTryWith() throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader("/Users/fangwei/temp/log.json1"))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void test() throws Exception {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("/Users/fangwei/temp/log.json1"));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            br.close();
        }
    }

}
