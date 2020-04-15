package com.vince.xq.common;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.*;

/**
 * @author
 * @Date 2019-11-13 14:26
 **/
@Slf4j
public class Utils {

    //消费kafka
    public static FlinkKafkaConsumer010<String> createConsumers(ParameterTool params, String topic) {
        Properties props = new Properties();

        String brokers = params.getProperties().getProperty("kafka.consumer.brokers");
        System.out.println(brokers);
        String groupid = params.getProperties().getProperty("kafka.channel.consumer.group.id");
        System.out.println(groupid);
        //String topic = params.getProperties().getProperty("kafka.web.info.topic");
        System.out.println(topic);
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", groupid);
        FlinkKafkaConsumer010<String> consumer010 =
                new FlinkKafkaConsumer010<>(topic, new org.apache.flink.api.common.serialization.SimpleStringSchema(), props);
        return consumer010;
    }

    public static ElasticsearchSink<String> createEsProducer(ParameterTool params, String index, String type) {
        List<HttpHost> httpHosts = new ArrayList<>();
        String url = params.getProperties().getProperty("es.http.url");
        int port = Integer.parseInt(params.getProperties().getProperty("es.http.port"));
        httpHosts.add(new HttpHost(url, port, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map map = JSONObject.parseObject(element, Map.class);
                        System.out.println(map);
                        return Requests.indexRequest()
                                .index(index)
                                .type(type)
                                .source(map);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                        @Override
                        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                            requestConfigBuilder.setConnectTimeout(5000);
                            requestConfigBuilder.setSocketTimeout(40000);
                            requestConfigBuilder.setConnectionRequestTimeout(1000);
                            return requestConfigBuilder;
                        }
                    }).setMaxRetryTimeoutMillis(5*60*1000);
                }
        );
        return esSinkBuilder.build();
    }

    /*public static FlinkKafkaConsumer010<String> createConsumers() {
        Properties props = new Properties();
        String brokers = "app-kafka.nsfly-kafka:31092";
        String groupid = "consumer-group23";
        String topic = "nts_design_canal.sys_web_log";
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", groupid);
        FlinkKafkaConsumer010<String> consumer010 =
                new FlinkKafkaConsumer010<>(topic, new org.apache.flink.api.common.serialization.SimpleStringSchema(), props);
        return consumer010;
    }*/
}
