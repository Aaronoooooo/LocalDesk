package ${packageName}

import com.gree.constant.Constant
import com.gree.model._
import com.gree.util.{EsUtil, JsonBeanUtil, KafkaUtil}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.{Logger, LoggerFactory}


object ${className} {
    def main(args: Array[String]): Unit = {
        val logger: Logger = LoggerFactory.getLogger(${className}.getClass)
        val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment

        //数据输出到ES的连接
        val httpConnect = EsUtil.getEsHttpConnect

        //设置任务并行度
        fsEnv.setParallelism(6)

        <#list sinks as sink>
        //循环多个表--start
        val ${sink.name} = new OutputTag[${sink.moduleName}](Constant.${sink.constantName})

        val ${sink.builderName} = new ElasticsearchSink.Builder[${sink.moduleName}](
            httpConnect,
            new ElasticsearchSinkFunction[${sink.moduleName}] {
                def process(${sink.moduleName}: ${sink.moduleName}, ctx: RuntimeContext, indexer: RequestIndexer) {
                    val json = JsonBeanUtil.getTblAzAssignLcLs(${sink.moduleName})
                    val rqst: IndexRequest = Requests.indexRequest
                        .index(Constant.${sink.esIndex})
                        .`type`(Constant.ES_TYPE)
                        .id(${sink.moduleName}.${sink.moduleColumn})
                        .source(json)
                    indexer.add(rqst)
                    logger.info("发送到es")
                }
            }
        )
        ${sink.builderName}.setBulkFlushMaxActions(Constant.ONE_HUNDRED)
        ${sink.builderName}.setBulkFlushMaxSizeMb(Constant.FIVE)
        ${sink.builderName}.setBulkFlushInterval(Constant.TWO_THOUSAND)

        val ${sink.streamName} = KafkaUtil.getKafkaConnect(Constant.${sink.kafkaTopic}, fsEnv)
        ${sink.streamName}.print("${sink.streamName}->")
        val ${sink.streamName1}: DataStream[${sink.moduleName}] = ${sink.streamName}.process(new ${sink.functionName1}(${sink.name}))
        ${sink.streamName1}.addSink(${sink.builderName}.build())

        val ${sink.sinkName}: DataStream[MaxKuanBiaoData] = ${sink.streamName1}.getSideOutput[${sink.moduleName}](${sink.name})
            .map(${sink.moduleName} => (${sink.moduleName}.${sink.moduleColumn}, ${sink.moduleName}))
            .process(new ${sink.functionName2}(${sink.name}, ${sink.name}))
        ${sink.sinkName}.addSink(${sink.builderName}.build())
        //end
        </#list>

        fsEnv.execute("安装宽表")
    }
}