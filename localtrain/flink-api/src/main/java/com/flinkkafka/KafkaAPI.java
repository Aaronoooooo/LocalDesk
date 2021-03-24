package com.flinkkafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author pengfeisu
 * @create 2020-03-05
 */
public class KafkaAPI {

    private static Logger logger = LoggerFactory.getLogger(KafkaAPI.class);

    //    public static String BOOTSTRAP_SERVERS = "106.52.33.54:9092";
//    public static String BOOTSTRAP_SERVERS = "flydiysz.cn:32670";
//    public static String BOOTSTRAP_SERVERS = "129.204.115.159:9092";
    public static String BOOTSTRAP_SERVERS = "node30:6667,node104:6667,node32:6667";
    public static String KAFKA_TOPIC = "nts_design_canal.test";
    public static String REQUEST_TIMEOUT = "6000";
    public static final Logger log = LoggerFactory.getLogger(KafkaAPI.class);
    //重试
    public static String RETRIES = "6";
    //分区
    public static int NUM_PARTITIONS = 1;
    //副本
    public static short REPLICATION_FACTOR = 1;

    public static void main(String[] args) {
        KafkaAPI kafka = new KafkaAPI();
//        kafka.creBulkTopic();
        kafka.listTopics();
//        kafka.createTopics();
//        kafka.creKafkaTopic();
//kafka.deleteTopics(KAFKA_TOPIC);
//kafka.listTopics();
//kafka.createTopics(KAFKA_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
//kafka.listTopics();
    }

    public static Properties getConfig() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(AdminClientConfig.RETRIES_CONFIG, RETRIES);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT);
// *prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
// *prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
// *prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
// *prop.put(ProducerConfig.ACKS_CONFIG, "all");
// *prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
// *prop.put(ProducerConfig.RETRIES_CONFIG, 0);

//连接broker集群地址，多个broker逗号隔开
// prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
 //是否自动ack
// prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
 //consumer向consunmer group发送心跳的超时时间，超过这个时间，consunmer group会将consunmer从组中移除，会发生rebalance.
// prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
 //心跳时间一般要小于session timeout时间的1/3
 //prop.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
// 消费者加入group时，消费offset设置策略
// earliest: 重置offset为最早的偏移地址，
// latest: 重置ofsset为已经消费的最大偏移，
// none: 没有offset时抛异常
// prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
 //多久自动提交一次ack,单位：ms
// prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
 //配置消费者所属的分组id
// prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
 //反序列化key
// prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
 //反序列化value
// prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
 //每次poll最多拉取多少条记录
// prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 8);
        return properties;
    }

    //查看主题
    public List listTopics() {
//        HashMap<String, String> map = new HashMap<>();
//map.put("createTopics","");
//map.put("numPartitions","");
//map.put("replicationFactor","");
        List list = new ArrayList<>();
        AdminClient client = AdminClient.create(getConfig());
        ListTopicsResult listTopics = client.listTopics();

        Set<String> strings = null;
        try {
            strings = listTopics.names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        strings.forEach((k) -> {
            System.out.println("kafkaTopic: " + k);
            list.add(k);
        });
//client.close();
        System.out.println("list: " + list);
        return list;
    }

    //创建主题
    public void createTopics(String createTopics, int numPartitions, short replicationFactor) {
        HashMap<String, String> map = new HashMap<>();

        AdminClient client = AdminClient.create(getConfig());
        Set<String> kafkaSet = null;
        try {
            kafkaSet = client.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        boolean contains = kafkaSet.contains(createTopics);
        if (!contains) {
            NewTopic newTopic = new NewTopic(createTopics, numPartitions, replicationFactor);
            //Collection<NewTopic> newTopicList = new ArrayList<>();
            //newTopicList.add(newTopic);
            //client.createTopics(newTopicList);
            client.createTopics(Arrays.asList(newTopic));
            logger.info(newTopic + "主题创建成功");
        } else {
            System.out.println(createTopics + "主题已存在");
        }
        client.close();
    }

    //删除主题
    public void deleteTopics(String deleteTopics) {
        AdminClient client = AdminClient.create(getConfig());
        List<String> allTopics = new LinkedList<>();
        try {
            allTopics.addAll(client.listTopics().names().get(60, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            logger.info("线程中断" + e);
        } catch (ExecutionException e) {
            logger.info("执行异常" + e);
        } catch (TimeoutException e) {
            logger.info("加载超时" + e);
        }

        boolean contains = allTopics.contains(deleteTopics);
        if (contains) {
            client.deleteTopics(Arrays.asList(deleteTopics));
            logger.info(deleteTopics + "主题删除成功");
        } else {
            logger.info(deleteTopics + "主题不存在");
        }
        client.close();
    }

    /*批量创建KafkaTopic*/
//    @Test
    public void createTopics() {
        AdminClient client = AdminClient.create(getConfig());
        String s = "{\"customTopic\":[{\"createTopics\":\"topic_test1\",\"numPartitions\":1,\"replicationFactor\":1},{\"createTopics\":\"topic_test2\",\"numPartitions\":1,\"replicationFactor\":1}]}";
        JSONObject jsonObject = JSON.parseObject(s);
        JSONArray customTopic = jsonObject.getJSONArray("customTopic");
        for (int i = 0; i < customTopic.size(); i++) {
            JSONObject object = customTopic.getJSONObject(i);
            String createTopics = object.getString("createTopics");
            int numPartitions = Integer.parseInt(object.getString("numPartitions"));
            short replicationFactor = Short.parseShort(object.getString("replicationFactor"));
            NewTopic newTopic = new NewTopic(createTopics, numPartitions, replicationFactor);
            client.createTopics(Arrays.asList(newTopic));
            System.out.println(createTopics + "主题创建成功");
        }
        client.close();
    }

    //    @Test
    public String creKafkaTopic() {

        String toJSONString = "{\"customTopic\":[{\"createTopics\":\"createTopics104\",\"numPartitions\":1,\"replicationFactor\":1},{\"createTopics\":\"createTopics103\",\"numPartitions\":1,\"replicationFactor\":1}],\"bootstrap.servers\":\"flydiysz.cn:31092\"}";
        JSONObject jsonObject = JSON.parseObject(toJSONString);
        String customTopic = jsonObject.getString("customTopic");
        String bootstrapServers = jsonObject.getString("bootstrap.servers");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient client = KafkaAdminClient.create(props);
        Set<String> creTopic = null;
        try {
            creTopic = client.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            JSONArray topicJsonArray = jsonObject.getJSONArray("customTopic");
            boolean contains;
            for (int i = topicJsonArray.size() - 1; i >= 0; i--) {
                JSONObject object = topicJsonArray.getJSONObject(i);
                String createTopics = object.getString("createTopics");
                int numPartitions = Integer.parseInt(object.getString("numPartitions"));
                short replicationFactor = Short.parseShort(object.getString("replicationFactor"));
                contains = creTopic.contains(createTopics);
                if (contains) {
                    log.info(createTopics + "主题已存在");
                } else {
                    NewTopic newTopic = new NewTopic(createTopics, numPartitions, replicationFactor);
                    client.createTopics(Arrays.asList(newTopic));
                    log.info("正在创建:" + createTopics + "主题");
                }
            }
            if (client != null) {
                client.close();
            }
        }
        return customTopic;
    }

    //    @Test
//    public void createTopics(List<String> topicNames, int numPartitions) {
    public void creBulkTopic() {
        AdminClient client = AdminClient.create(getConfig());
        List<String> creAllTopic = new LinkedList<>();
        creAllTopic.add("test1");
        creAllTopic.add("test2");
        List<NewTopic> newTopics = new ArrayList<>();
        for (String topic : creAllTopic) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            newTopics.add(newTopic);
        }
        client.createTopics(newTopics);
        logger.info(newTopics.toString() + "主题创建成功");

        //the following lines are a bit of black magic to ensure the topic is ready when we return
//        DescribeTopicsResult dtr = client.describeTopics(creAllTopic);
//        try {
//            dtr.all().get(10, TimeUnit.SECONDS);
//        } catch (Exception e) {
//            throw new RuntimeException("Error getting topic info", e);
//        }
        client.close();
    }
}