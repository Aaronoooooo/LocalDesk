package com.fwmagic.flink.watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * aa 1557547200000
 * aa 1557547205000
 * aa 1557547210000
 * aa 1557547219000
 * aa 1557547226000
 * aa 1557547227000
 * aa 1557547228000
 * aa 1557547800000
 */
public class GetWaterMarkTime {

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time1 = sdf.parse("2019-05-11 12:00:00").getTime();
        long time2 = sdf.parse("2019-05-11 12:00:05").getTime();
        long time3 = sdf.parse("2019-05-11 12:00:10").getTime();
        long time4 = sdf.parse("2019-05-11 12:00:19").getTime();
        long time5 = sdf.parse("2019-05-11 12:00:26").getTime();
        long time6 = sdf.parse("2019-05-11 12:00:27").getTime();
        long time7 = sdf.parse("2019-05-11 12:00:28").getTime();
        long time8 = sdf.parse("2019-05-11 12:10:00").getTime();

        System.out.println("aa "+time1);
        System.out.println("aa "+time2);
        System.out.println("aa "+time3);
        System.out.println("aa "+time4);
        System.out.println("aa "+time5);
        System.out.println("aa "+time6);
        System.out.println("aa "+time7);
        System.out.println("aa "+time8);
    }
}
