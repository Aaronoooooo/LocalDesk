package com.fwmagic.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 读取本地文件
 */
public class ReadLocalFileWC {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("/Users/fangwei/temp/aaa");
        dataSource.flatMap(new FlatMapFunction<String, WordCount>() {
            public void flatMap(String line, Collector<WordCount> collector) {
                String[] split = line.split("\\s");
                for (String word : split) {
                    collector.collect(new WordCount(word, 1L));
                }
            }
        }).groupBy("word").reduce(new ReduceFunction<WordCount>() {
            public WordCount reduce(WordCount w1, WordCount w2) {
                return new WordCount(w1.word, w1.count + w2.count);
            }
        }).setParallelism(1).print();

    }

    public static class WordCount {
        public String word;
        public Long count;

        public WordCount() {
        }

        public WordCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}


