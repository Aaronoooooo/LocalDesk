package ${packageName}

import java.util.Properties
import com.gree.constant.Constant
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig


/**
* KAFKA工具类
*/
object KafkaUtil {

    val conf = ConfigurationUtil(Constant.CONFIG_PROPERTIES)
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString(Constant.KAFKA_HOST))
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constant.KAFKA_GROUP_ID)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,Constant.KAFKA_AUTO_OFFSET_RESET)
    /* properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Constant.KAFKA_BROKER_LIST)*/

    /**
    * 获取Kafka连接
    */
    def getKafkaConnect(KafkaTopic:String,fsEnv:StreamExecutionEnvironment):DataStream[ObjectNode] = {
        val value:DataStream[ObjectNode]= fsEnv.addSource(new FlinkKafkaConsumer(KafkaTopic, new JSONKeyValueDeserializationSchema(true), properties))
        value
    }
}
