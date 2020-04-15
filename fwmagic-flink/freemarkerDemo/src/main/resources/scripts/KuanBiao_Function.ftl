package ${packageName}

import java.text.SimpleDateFormat
import java.util.Calendar
import com.gree.model._
import com.gree.util.{EsConnection, NumberFormatUtil}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._

class ${className}(${mingxi}:OutputTag[${mingxiModule}],${caiwu}:OutputTag[${caiwuModule}]) extends ProcessFunction[(String,${moduleName}),${maxModule}]{

    //主表信息拼接宽表
    override def processElement(value: (String, ${moduleName}), ctx: ProcessFunction[(String, ${moduleName}), ${maxModule}]#Context, out: Collector[${maxModule}]): Unit = {

        val logger: Logger = LoggerFactory.getLogger(${className}.super.getClass)
        //链接ES
        val client: TransportClient = EsConnection.conn

        //反查安装主表ES
        val ${zhubiao}: SearchResponse = client
            .prepareSearch("${zhubiaoIndex}")
            .setTypes("${esType}")
            .setQuery(QueryBuilders.termQuery("${esColumn}", value._1)) //直接查询即可,只对应一条
            .get()

        val ${zhubiao}Hits = ${zhubiao}.getHits

        //反查安装反馈明细表ES
        val ${fxmx}: SearchResponse = client
            .prepareSearch("${fxmxIndex}")
            .setTypes("${esType}")
            .setQuery(QueryBuilders.termQuery("${esColumn}", value._1))
            .addSort("${esOrder}",SortOrder.ASC)   //反馈类别like完工的并取最老的一条数据
            .setFrom(0)
            .setSize(1)
            .get()

        val ${fxmx}Hits = ${fxmx}.getHits

        //反查安装明细表ES
        val ${mx}: SearchResponse = client
            .prepareSearch("${mxIndex}")
            .setTypes("${esType}")
            .setQuery(QueryBuilders.termQuery("${esColumn}", value._1)) //直接查询即可,只对应一条
            .get()

        val ${mx}Hits = ${mx}.getHits

        <#list zhubiaocolumns as column>
        var ${column.columnCode}: String = "null"
        </#list>
        <#list fxmxcolumns as column>
        var ${column.columnCode}: String = "null"
        </#list>
        <#list mingxicolumns as column>
        var ${column.columnCode}: String = "null"
        </#list>
        var azChanPinMingXi:String =""

        //反查主表
        if (${zhubiao}Hits.iterator().hasNext){
            val hit = ${zhubiao}Hits.iterator().next()
            try {
                <#list zhubiaocolumns as column>
                ${column.columnCode} = hit.getSourceAsMap.get("${column.columnCode}").toString
                </#list>
            } catch {
                case e:Exception =>logger.error("${className}从安装主表查询异常->"+e.getMessage)
            }
        }

        //反查安装反馈明细表ES
        if (${fxmx}Hits.iterator().hasNext){
            val hit = ${fxmx}Hits.iterator().next()
            try {
                <#list fxmxcolumns as column>
                ${column.columnCode} = hit.getSourceAsMap.get("${column.columnCode}").toString
                </#list>
            } catch {
                case e:Exception => logger.error("${className}查明安装反馈细表异常->"+e.getMessage)
            }
        }

        val util = new NumberFormatUtil
        //要求完工时间
        //如果预约时间为空，则要求完工时间为创建时间加一天
        try {
            if ("null".equals(value._2.kssj)) {
                val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val parse = simpleDateFormat.parse(util.panduanDate(cjdt))
                val calendar = Calendar.getInstance
                calendar.setTime(parse)
                calendar.add(Calendar.DATE, 1)
                yqwangongtime = simpleDateFormat.format(calendar.getTime())

            //完工时间为预约时间加一天
            } else {
                val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val parse = simpleDateFormat.parse(util.panduanDate(value._2.kssj))
                val calendar = Calendar.getInstance
                calendar.setTime(parse)
                calendar.add(Calendar.DATE, 1)
                yqwangongtime = simpleDateFormat.format(calendar.getTime())
            }
        } catch {
            case e:Exception =>logger.error("${className}时间转换异常—>"+e.getMessage)
        }


        //输出最大宽表
        out.collect(${maxModule}(
            <#list maxcolumns as column>
            ${column.columnCode}<#if column_has_next>,</#if>
            </#list>
        ))

        var count:Int = 1
        //反查安装明细表ES
        for (mingxitable <- ${mx}Hits ){

            logger.info("${className}循环方法->"+count)
            try {
                <#list mingxicolumns as column>
                ${column.columnCode} = mingxitable.getSourceAsMap.get("${column.columnCode}").toString
                </#list>
    
            //循环输出财务宽表
            ctx.output[${caiwuModule}](${caiwu},${caiwuModule}(
                <#list caiwucolumns as column>
                ${column.columnCode}<#if column_has_next>,</#if>
                </#list>
            ))

        } catch {
            case e:Exception => logger.error("${className}查明安装细表异常->"+e.getMessage)
        }
        var aaa:String = ""

        aaa = "TblAzAssignMx_"+count+":" +
            <#list mxcolumns as column>
            ",${column.columnCode}:" + ${column.columnCode} +
            </#list>
           "。"

        azChanPinMingXi = azChanPinMingXi + aaa

        count += 1
        }

        //输出明细宽表
        ctx.output[${mingxiModule}](${mingxi},${mingxiModule}(
            <#list mingxicolumns as column>
            ${column.columnCode}<#if column_has_next>,</#if>
            </#list>
        ))

    }

}
