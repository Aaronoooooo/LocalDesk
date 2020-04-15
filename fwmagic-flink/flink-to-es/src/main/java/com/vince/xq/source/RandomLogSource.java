package com.vince.xq.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author
 * @Date 2019-11-14 13:02
 **/
public class RandomLogSource extends RichSourceFunction<String> {
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        while (isRunning) {
            String str = "{\"data\":[{\"version\":\"6\",\"id\":\"100102\",\"corpId\":\"1001\",\"corpIdVer\":\"1\",\"createTime\":\"2017-11-04 09:26:00\",\"creator\":\"chenjh\",\"creatorVer\":\"2\",\"flowRoleId\":\"\",\"flowRoleIdVer\":null,\"flowStatus\":\"\",\"isNew\":\"1\",\"modiTime\":\"2017-12-08 15:57:00\",\"modiUser\":\"a00001ac11000515084623585362220\",\"modiUserVer\":\"10\",\"orgId\":\"123456\",\"orgIdVer\":\"1\",\"tenantId\":\"1001\",\"tenantIdVer\":\"1\",\"addr\":\"\",\"code\":\"10102\",\"fax\":\"\",\"isCorp\":\"1\",\"isCost\":\"0\",\"isFinance\":\"0\",\"isProfit\":\"0\",\"isRepertory\":\"0\",\"leader\":\"a00001ac11000515114095474221186\",\"leaderVer\":null,\"name\":\"项目组\",\"parentId\":\"1001\",\"parentIdVer\":\"3\",\"remark\":\"11\",\"shortName\":\"\",\"sort\":\"1\",\"status\":\"valid\",\"tel\":\"\",\"type\":\"org\",\"typeDesc\":\"小组\",\"web\":null,\"zip\":null}],\"database\":\"nts_design_canal\",\"es\":1572708925000,\"id\":1,\"isDdl\":false,\"mysqlType\":{\"version\":\"int(11)\",\"id\":\"varchar(64)\",\"corpId\":\"varchar(64)\",\"corpIdVer\":\"int(11)\",\"createTime\":\"datetime\",\"creator\":\"varchar(64)\",\"creatorVer\":\"int(11)\",\"flowRoleId\":\"varchar(64)\",\"flowRoleIdVer\":\"int(11)\",\"flowStatus\":\"varchar(255)\",\"isNew\":\"int(11)\",\"modiTime\":\"datetime\",\"modiUser\":\"varchar(64)\",\"modiUserVer\":\"int(11)\",\"orgId\":\"varchar(64)\",\"orgIdVer\":\"int(11)\",\"tenantId\":\"varchar(64)\",\"tenantIdVer\":\"int(11)\",\"addr\":\"varchar(100)\",\"code\":\"varchar(100)\",\"fax\":\"varchar(24)\",\"isCorp\":\"int(11)\",\"isCost\":\"int(11)\",\"isFinance\":\"int(11)\",\"isProfit\":\"int(11)\",\"isRepertory\":\"int(11)\",\"leader\":\"varchar(64)\",\"leaderVer\":\"int(11)\",\"name\":\"varchar(150)\",\"parentId\":\"varchar(64)\",\"parentIdVer\":\"int(11)\",\"remark\":\"varchar(4000)\",\"shortName\":\"varchar(100)\",\"sort\":\"int(11)\",\"status\":\"varchar(32)\",\"tel\":\"varchar(24)\",\"type\":\"varchar(32)\",\"typeDesc\":\"varchar(32)\",\"web\":\"varchar(150)\",\"zip\":\"varchar(20)\"},\"old\":[{\"name\":\"项目组11\"}],\"pkNames\":[\"version\",\"id\"],\"sql\":\"\",\"sqlType\":{\"version\":4,\"id\":12,\"corpId\":12,\"corpIdVer\":4,\"createTime\":93,\"creator\":12,\"creatorVer\":4,\"flowRoleId\":12,\"flowRoleIdVer\":4,\"flowStatus\":12,\"isNew\":4,\"modiTime\":93,\"modiUser\":12,\"modiUserVer\":4,\"orgId\":12,\"orgIdVer\":4,\"tenantId\":12,\"tenantIdVer\":4,\"addr\":12,\"code\":12,\"fax\":12,\"isCorp\":4,\"isCost\":4,\"isFinance\":4,\"isProfit\":4,\"isRepertory\":4,\"leader\":12,\"leaderVer\":4,\"name\":12,\"parentId\":12,\"parentIdVer\":4,\"remark\":12,\"shortName\":12,\"sort\":4,\"status\":12,\"tel\":12,\"type\":12,\"typeDesc\":12,\"web\":12,\"zip\":12},\"table\":\"sys_org\",\"ts\":1572932231090,\"type\":\"UPDATE\"}";
            sourceContext.collect(str);
            isRunning = false;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
