package com.gree.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class SplitUtil {

    public static Logger logger = LoggerFactory.getLogger("SplitUtil");

    public static ArrayList<String> SplitFunctionFor8(String s){


        ArrayList<String> list = new ArrayList<String>();

        for (int i = 0; i <= (s.length()/8-1); i++) {
            if(i==0){

                logger.info("循环"+i+"次 结果为"+s.substring(0,8));
                list.add(0,s.substring(0,8));
            }
            if(i==1){
                logger.info("循环"+i+"次 结果为"+s.substring(8,16));
                list.add(1,s.substring(8,16));
            }
            if(i==2){
                logger.info("循环"+i+"次 结果为"+s.substring(16,24));
                list.add(2,s.substring(16,24));
            }
            if(i==3){
                logger.info("循环"+i+"次 结果为"+s.substring(24,32));
                list.add(3,s.substring(24,32));
            }
            if(i==4){
                logger.info("循环"+i+"次 结果为"+s.substring(32,40));
                list.add(4,s.substring(32,40));
            }
            if(i==5){
                logger.info("循环"+i+"次 结果为"+s.substring(40,48));
                list.add(5,s.substring(40,48));
            }
            if(i==6){
                logger.info("循环"+i+"次 结果为"+s.substring(48,56));
                list.add(6,s.substring(48,56));
            }


        }

            return list;

    }
}
