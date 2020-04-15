package com.flinksql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.util.*;

public class StreamSQLExample {

    public static void main(String[] args) throws Exception {

        //构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        //连接kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "flydiysz.cn:31092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "3s42");
//        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("auto.offset.reset", "latest");

        //连接es
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("flydiysz.cn", 52200, "http"));
//        httpHosts.add(new HttpHost("cdh102", 52200, "http"));
//        httpHosts.add(new HttpHost("cdh103", 52200, "http"));

        //生产者
        DataStreamSource<String> produce1 = env.readTextFile("src/main/resources/produce1.txt");
        produce1.addSink(new FlinkKafkaProducer<String>("nts_design_canal.teacher", new SimpleStringSchema(), properties));

        DataStreamSource<String> produce2 = env.readTextFile("src/main/resources/produce2.txt");
        produce2.addSink(new FlinkKafkaProducer<String>("nts_design_canal.student", new SimpleStringSchema(), properties));

        /**
         * 消费org表
         */
        DataStream<String> inputStream1 = env.addSource(new FlinkKafkaConsumer<>("nts_design_canal.student", new SimpleStringSchema(), properties));
        inputStream1.print("sys_org_topic").setParallelism(1);

        //取出org表数据
        DataStream<OrgModel> orgMapperStream = inputStream1.flatMap(new RichFlatMapFunction<String, OrgModel>() {
            @Override
            public void flatMap(String value, Collector<OrgModel> collector) {
                JSONObject jsonObject = JSONObject.parseObject(value);

                JSONArray jsonArray = JSONArray.parseArray(jsonObject.get("data").toString());
                String ts = jsonObject.get("ts").toString();

                JSONObject object = JSONObject.parseObject(jsonArray.get(0).toString());

                OrgModel orgModel = new OrgModel();
                orgModel.setId(object.getString("id"));
                orgModel.setOrgId(object.getString("orgId"));
                orgModel.setName(object.getString("name"));
                orgModel.setOrgTs(ts);

                collector.collect(orgModel);
            }
        });


        //注册org table
        tEnv.registerDataStream("orgtable", orgMapperStream);
        Table org = tEnv.sqlQuery("select * from orgtable");
        org.printSchema();
        DataStream<OrgModel> orgModelDataStream = tEnv.toAppendStream(org, OrgModel.class);

        SingleOutputStreamOperator<OrgModel> orgModelDataStreamMap = orgModelDataStream.map((MapFunction<OrgModel, OrgModel>) value -> value);
        orgModelDataStreamMap.print("org_table").setParallelism(1);

        //构建orgModelBuilder
        ElasticsearchSink.Builder<OrgModel> orgModelBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<OrgModel>() {
                    public IndexRequest createIndexRequest(OrgModel orgModel) {
                        Map json = new HashMap<String, OrgModel>();
                        json.put("id", orgModel.id);
                        json.put("orgId", orgModel.orgId);
                        json.put("name", orgModel.name);
                        json.put("orgTs", orgModel.orgTs);

                        return Requests.indexRequest()
                                .index("orgmodel-index")
                                .type("_doc")
                                .source(json);
                    }

                    @Override
                    public void process(OrgModel orgModel, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(orgModel));
                    }
                }
        );

        /**
         * 消费user表
         */
        DataStream<String> inputStream2 = env.addSource(new FlinkKafkaConsumer<>("nts_design_canal.teacher", new SimpleStringSchema(), properties));
        inputStream2.print("sys_user_topic").setParallelism(1);

        //取出user表数据
        DataStream<UserModel> userMapperStream = inputStream2.flatMap(new RichFlatMapFunction<String, UserModel>() {
            @Override
            public void flatMap(String value, Collector<UserModel> collector) {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String ts = jsonObject.get("ts").toString();

                JSONArray jsonArray = JSONArray.parseArray(jsonObject.get("data").toString());
                JSONObject object = JSONObject.parseObject(jsonArray.get(0).toString());

                UserModel userModel = new UserModel();
                userModel.setId(object.getString("id"));
                userModel.setRealName(object.getString("realName"));
                userModel.setUserTs(ts);
                collector.collect(userModel);
            }
        });

        //注册user table
        tEnv.registerDataStream("usertable", userMapperStream);
        Table user = tEnv.sqlQuery("select * from usertable");
        user.printSchema();
        DataStream<UserModel> userModelTableDataStream = tEnv.toAppendStream(user, UserModel.class);

        SingleOutputStreamOperator<UserModel> userModelTableDataStreamMap = userModelTableDataStream.map(new MapFunction<UserModel, UserModel>() {
            @Override
            public UserModel map(UserModel value) throws Exception {
                return value;
            }
        });
        userModelTableDataStreamMap.print("user_table").setParallelism(1);

        //构建userModelBuilder
        ElasticsearchSink.Builder<UserModel> userModelBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<UserModel>() {
                    public IndexRequest createIndexRequest(UserModel userModel) {
                        Map json = new HashMap<String, UserModel>();
                        json.put("id", userModel.id);
                        json.put("realName", userModel.realName);
                        json.put("userTs", userModel.userTs);

                        return Requests.indexRequest()
                                .index("usermodel-index")
                                .type("_doc")
                                .source(json);
                    }

                    @Override
                    public void process(UserModel userModel, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(userModel));
                    }
                }
        );

        //注册宽表
        Table wideTable = tEnv.sqlQuery("select * from usertable u join orgtable o on u.id = o.orgId");
        wideTable.printSchema();
        DataStream<WideTable> wideTableDataStream = tEnv.toAppendStream(wideTable, WideTable.class);

        SingleOutputStreamOperator<WideTable> wideTableDataStreamMap = wideTableDataStream.map(new MapFunction<WideTable, WideTable>() {
            @Override
            public WideTable map(WideTable value) throws Exception {
                return value;
            }
        });
        wideTableDataStreamMap.print("wide_table").setParallelism(1);

        //构建WideTableBuilder
        ElasticsearchSink.Builder<WideTable> wideTableBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<WideTable>() {
                    public IndexRequest createIndexRequest(WideTable wideTable) {
                        Map json = new HashMap<String, WideTable>();
                        json.put("id", wideTable.id0);
                        json.put("id0", wideTable.id);
                        json.put("orgId", wideTable.orgId);
                        json.put("name", wideTable.name);
                        json.put("realName", wideTable.realName);
                        json.put("orgTs", wideTable.orgTs);
                        json.put("userTs", wideTable.userTs);

                        return Requests.indexRequest()
                                .index("widetable-index")
                                .type("_doc")
                                .source(json);
                    }

                    @Override
                    public void process(WideTable wideTable, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(wideTable));
                    }
                }
        );

        // 设置sink到es参数
        orgModelBuilder.setBulkFlushMaxActions(1);
        userModelBuilder.setBulkFlushMaxActions(1);
        wideTableBuilder.setBulkFlushMaxActions(1);

        // 设置索引文档参数
        orgModelBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                        @Override
                        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                            requestConfigBuilder.setConnectTimeout(50001);
                            requestConfigBuilder.setSocketTimeout(40000);
                            requestConfigBuilder.setConnectionRequestTimeout(1000);
                            return requestConfigBuilder;
                        }
                    }).setMaxRetryTimeoutMillis(5 * 60 * 1000);
                }
        );
        userModelBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                        @Override
                        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                            requestConfigBuilder.setConnectTimeout(50002);
                            requestConfigBuilder.setSocketTimeout(40000);
                            requestConfigBuilder.setConnectionRequestTimeout(1000);
                            return requestConfigBuilder;
                        }
                    }).setMaxRetryTimeoutMillis(5 * 60 * 1000);
                }
        );
        wideTableBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                        @Override
                        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                            requestConfigBuilder.setConnectTimeout(50003);
                            requestConfigBuilder.setSocketTimeout(40000);
                            requestConfigBuilder.setConnectionRequestTimeout(1000);
                            return requestConfigBuilder;
                        }
                    }).setMaxRetryTimeoutMillis(5 * 60 * 1000);
                }
        );

        // 写入es
        orgModelDataStreamMap.addSink(orgModelBuilder.build());
        userModelTableDataStreamMap.addSink(userModelBuilder.build());
        wideTableDataStreamMap.addSink(wideTableBuilder.build());

        //执行
        env.execute("flink-sql");
    }
}