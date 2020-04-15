package ${packageName}

import java.util
import com.gree._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class ${className}(${stream}: OutputTag[${tableName}]) extends ProcessFunction[ObjectNode, ${tableName}] {
    override def processElement(value: ObjectNode, ctx: ProcessFunction[ObjectNode, ${tableName}]#Context, out: Collector[${tableName}]): Unit = {

        //data中可能有多个对象，遍历取出
        val logger: Logger = LoggerFactory.getLogger(${className}.super.getClass)
        val data: util.Iterator[JsonNode] = value.get("value").get("data").elements()
        val caoZuoType = value.get("value").get("type").asText()

        while (data.hasNext) {
            val zhubiao: JsonNode = data.next()
            //需要同步的数据
            try {
                out.collect(${tableName}(
                    <#list columns as column>
                    zhubiao.get("${column.columnCode}").asText(),
                    </#list>
                    caoZuoType))

                Thread.sleep(700)

                //需要进行逻辑计算的数据
                ctx.output[${tableName}](${stream}, ${tableName}(
                    <#list columns as column>
                    zhubiao.get("${column.columnCode}").asText(),
                    </#list>
                    caoZuoType))
                } catch {
                case e:Exception =>logger.error("${className}抓取数据异常->"+e.getMessage)
            }
        }
    }

}
