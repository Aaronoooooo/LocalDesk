package com.javatime;

import java.text.SimpleDateFormat;
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
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String formatTime = sdf.format(new Date());

        StringBuffer branchName1 = new StringBuffer();
        branchName1.append("AutoCode").append("_").append(formatTime);
        System.out.println(branchName1);

        StringBuilder branchName2 = new StringBuilder();
        branchName2.append("AutoCode").append("___").append(formatTime);

        System.out.println(branchName2);

    }
}
