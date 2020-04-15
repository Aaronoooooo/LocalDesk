package com.vince.xq;

import com.vince.xq.common.Configs;
import com.vince.xq.common.Utils;
import com.vince.xq.mapper.OrgMapper;
import com.vince.xq.source.RandomOrgSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author
 * @Date 2019-11-13 15:48
 **/
public class OrgToEsJob {
    public static void main(String[] args) throws Exception {
        /*if (args == null || args.length == 0) {
            throw new RuntimeException("config file name must be config, config is args[0]");
        }*/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = Configs.loadConfig("config-test.properties");
        String topic = parameterTool.getProperties().getProperty("kafka.org.info.topic");
        env.getConfig().setGlobalJobParameters(parameterTool);
        //DataStream<String> orgStream = env.addSource(new RandomOrgSource());
        DataStream<String> orgStream = env.addSource(Utils.createConsumers(parameterTool, topic));
        DataStream<String> orgMapperStream = orgStream.flatMap(new OrgMapper());
        String index = "sys_org_test";
        String type = "org";
        orgMapperStream.addSink(Utils.createEsProducer(parameterTool, index, type));
        env.execute("org-to-es");
    }
}
