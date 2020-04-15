package com.vince.xq.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author
 * @Date 2019-11-13 14:02
 **/
public class UserSink extends RichSinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(1);
        System.out.println(value);
    }

}
