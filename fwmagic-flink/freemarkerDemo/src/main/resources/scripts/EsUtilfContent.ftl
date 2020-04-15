package ${packageName}

import java.net.InetAddress

import com.gree.constant.Constant
import org.apache.http.HttpHost
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
* ES工具类
*/
object EsUtil extends  Serializable {
    val conf = ConfigurationUtil(Constant.CONFIG_PROPERTIES)

    def getEsConnect: TransportClient = {
        val settings: Settings = Settings.builder()
            .put("cluster.name", "${clusterName}")
            .build()

        //连接ES集群
        val client: TransportClient = new PreBuiltTransportClient(settings)
        .addTransportAddresses(
            <#list hosts as host>
            new TransportAddress(InetAddress.getByName("${host.url}"), ${host.port})<#if host_has_next>,</#if>
            </#list>
        )
    client
    }

    /**
    * 获取ES http连接
    */
    def getEsHttpConnect:java.util.ArrayList[HttpHost]={
        val httpHosts = new java.util.ArrayList[HttpHost]
        <#list hosts as host>
        httpHosts.add(new HttpHost(${host.url}, ${host.port}, Constant.ES_SCHEME))
        </#list>
        httpHosts
    }
}
