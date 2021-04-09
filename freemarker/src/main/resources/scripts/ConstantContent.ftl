package ${packageName};


/**
* 常量类
*/
public class Constant {

    public static final int ES_PROT = ${es_prot};
    public static final int ES_HTTP_PROT = ${es_http_prot};
    public static final int ONE_HUNDRED = ${one_hundred};
    public static final int FIVE = ${five};
    public static final int TWO_THOUSAND = ${two_thousand};
    public static final String ES_SCHEME = "${es_scheme}";
    public static final String CLUSTER_NAME = "${cluster_name}";
    public static final String CONFIG_PROPERTIES = "${config_properties}";
    <#list es_hosts as es_host>
    public static final String ES_HOST = "${es_host.url}";
    </#list>
    public static final String KAFKA_BROKER_LIST = "${kafka_broker_list}";
    public static final String KAFKA_HOST = "${kafka_host}";
    public static final String KAFKA_GROUP_ID = "${kafka_group_id}";
    public static final String KAFKA_AUTO_OFFSET_RESET = "${kafka_auto_offset_reset}";
    public static final boolean KAFKA_ENABLE_AUTO_COMMIT = ${kafka_enable_auto_commit};
    public static final String ES_TYPE = "${es_type}";

    //安装
    <#list modules as module>
    public static final String ${module.name} = "${module.name}";
    </#list>
}