package com.vince.xq;

import com.vince.xq.common.Configs;
import com.vince.xq.common.Utils;
import com.vince.xq.mapper.UserMapper;
import com.vince.xq.source.RandomUserSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author
 * @Date 2019-11-13 13:51
 **/
@Slf4j
public class UserToEsJob {
    public static void main(String[] args) throws Exception {
        /*if (args == null || args.length == 0) {
            throw new RuntimeException("config file name must be config, config is args[0]");
        }*/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<String> userStream = env.addSource(new RandomUserSource());

        ParameterTool parameterTool = Configs.loadConfig("config-test.properties");
        env.getConfig().setGlobalJobParameters(parameterTool);
        String topic = parameterTool.getProperties().getProperty("kafka.user.info.topic");
        DataStream<String> userStream = env.addSource(Utils.createConsumers(parameterTool, topic));
        DataStream<String> userMapperStream = userStream.flatMap(new UserMapper());
        String index = "sys_user_test";
        String type = "user";
        userMapperStream.addSink(Utils.createEsProducer(parameterTool, index, type));

        env.execute("user-to-es");
    }
}
