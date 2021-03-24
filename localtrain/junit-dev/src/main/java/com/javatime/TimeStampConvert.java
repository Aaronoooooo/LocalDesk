package com.javatime;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author pengfeisu
 * @create 2020-02-14 11:44
 */
public class TimeStampConvert {
    public static void main(String[] args) {

        String branch = "AutoCode";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String formatTime = sdf.format(new Date());
        //String executeTime = sdfYMD.format(new Date() + " " + "00:01:05");
        Calendar instance = Calendar.getInstance();
        System.out.println("instance:" + instance);

        //instance.set(Calendar.HOUR_OF_DAY, instance.get(Calendar.HOUR_OF_DAY) - 2);
        instance.set(Calendar.HOUR_OF_DAY,-2);
        String forTime = sdf.format(instance.getTime());
        System.out.println("当前时间减去2小时后:" + forTime);

        StringBuffer branchName1 = new StringBuffer();
        branchName1.append("AutoCode").append("_").append(formatTime);
        System.out.println(branchName1);

        StringBuilder branchName2 = new StringBuilder();
        branchName2.append("AutoCode").append("___").append(formatTime);

        System.out.println(branchName2);

    }
}
