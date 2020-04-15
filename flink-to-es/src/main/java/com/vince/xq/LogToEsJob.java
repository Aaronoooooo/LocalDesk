package com.vince.xq;

import com.vince.xq.common.Configs;
import com.vince.xq.common.Utils;
import com.vince.xq.mapper.LogMapper;
import com.vince.xq.source.RandomLogSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author
 * @Date 2019-11-13 15:50
 **/
public class LogToEsJob {
    public static void main(String[] args) throws Exception {
        /*if (args == null || args.length == 0) {
            throw new RuntimeException("config file name must be config, config is args[0]");
        }*/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = Configs.loadConfig("config-test.properties");
        env.getConfig().setGlobalJobParameters(parameterTool);
        String topic = parameterTool.getProperties().getProperty("kafka.log.info.topic");
        DataStream<String> logStream = env.addSource(Utils.createConsumers(parameterTool, topic));
//        DataStream<String> logStream = env.addSource(new RandomLogSource());

        DataStream<String> logMapperStream = logStream.flatMap(new LogMapper());
        String index = "sys_web_log_test";
        String type = "log";
        logMapperStream.addSink(Utils.createEsProducer(parameterTool, index, type));
        env.execute("log-to-es");
    }
}
