package ${packageName}

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

object EsConnection {

    val logger: Logger = LoggerFactory.getLogger(EsConnection.super.getClass)
    def createESConnection():TransportClient = {
        val settings: Settings = Settings.builder()
            .put("cluster.name", "${clusterName}")
            .build()

        val client: TransportClient = new PreBuiltTransportClient(settings)
            .addTransportAddresses(
            <#list hosts as host>
            new TransportAddress(InetAddress.getByName("${host.url}"), ${host.port})<#if host_has_next>,</#if>
            </#list>
        )
        client
    }

    val conn:TransportClient = createESConnection()

    Runtime.getRuntime.addShutdownHook(new Thread(){
        override def run(): Unit = {
            logger.info("close")
            conn.close()
        }
    })

}
