package com.fwmagic.flink.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 分布式缓存文件添加和获取
 */
public class BatchDemoDisCache {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //注册一个缓存文件
        env.registerCachedFile("/Users/fangwei/temp/log.json", "log.json");

        DataSource<String> data = env.fromElements("a", "b", "c");

        DataSet<String> dataSet = data.map(new RichMapFunction<String, String>() {

            List<String> list = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("log.json");
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    list.add(line);
                    System.out.println("line:" + line);
                }
            }

            @Override
            public String map(String data) throws Exception {
                System.out.println("data:" + data);
                String s = "";
                for (String l : list) {
                    s = "=>l : data =" + l + " : " + data;
                }
                return s;
            }
        });

        //打印
        dataSet.print();
    }
}
