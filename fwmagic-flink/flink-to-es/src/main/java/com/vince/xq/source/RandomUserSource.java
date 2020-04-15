package com.vince.xq.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author
 * @Date 2019-11-14 13:02
 **/
public class RandomUserSource extends RichSourceFunction<String> {
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        while (isRunning) {
            Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);

            String str = "{\"data\":[{\"version\":\"1\",\"id\":\"chenjh\",\"corpId\":null,\"corpIdVer\":null,\"createTime\":null,\"creator\":null,\"creatorVer\":null,\"flowRoleId\":null,\"flowRoleIdVer\":null,\"flowStatus\":null,\"isNew\":\"1\",\"modiTime\":null,\"modiUser\":null,\"modiUserVer\":null,\"orgId\":\"1001\",\"orgIdVer\":null,\"tenantId\":\"1001\",\"tenantIdVer\":null,\"account\":\"chenjh\",\"avatar\":null,\"code\":\"12345\",\"email\":\"123\",\"errorCount\":null,\"errorTime\":null,\"jobsId\":null,\"jobsIdVer\":null,\"mobile\":null,\"password\":null,\"pwdModiTime\":null,\"realName\":\"陈进辉\",\"sex\":null,\"status\":\"valid\"}],\"database\":\"nts_design_canal\",\"es\":1572932313000,\"id\":2,\"isDdl\":false,\"mysqlType\":{\"version\":\"int(11)\",\"id\":\"varchar(64)\",\"corpId\":\"varchar(64)\",\"corpIdVer\":\"int(11)\",\"createTime\":\"datetime\",\"creator\":\"varchar(64)\",\"creatorVer\":\"int(11)\",\"flowRoleId\":\"varchar(64)\",\"flowRoleIdVer\":\"int(11)\",\"flowStatus\":\"varchar(255)\",\"isNew\":\"int(11)\",\"modiTime\":\"datetime\",\"modiUser\":\"varchar(64)\",\"modiUserVer\":\"int(11)\",\"orgId\":\"varchar(64)\",\"orgIdVer\":\"int(11)\",\"tenantId\":\"varchar(64)\",\"tenantIdVer\":\"int(11)\",\"account\":\"varchar(100)\",\"avatar\":\"longtext\",\"code\":\"varchar(100)\",\"email\":\"varchar(50)\",\"errorCount\":\"int(11)\",\"errorTime\":\"datetime\",\"jobsId\":\"varchar(64)\",\"jobsIdVer\":\"int(11)\",\"mobile\":\"varchar(24)\",\"password\":\"varchar(300)\",\"pwdModiTime\":\"datetime\",\"realName\":\"varchar(100)\",\"sex\":\"varchar(24)\",\"status\":\"varchar(24)\"},\"old\":[{\"code\":\"1234\"}],\"pkNames\":[\"version\",\"id\"],\"sql\":\"\",\"sqlType\":{\"version\":4,\"id\":12,\"corpId\":12,\"corpIdVer\":4,\"createTime\":93,\"creator\":12,\"creatorVer\":4,\"flowRoleId\":12,\"flowRoleIdVer\":4,\"flowStatus\":12,\"isNew\":4,\"modiTime\":93,\"modiUser\":12,\"modiUserVer\":4,\"orgId\":12,\"orgIdVer\":4,\"tenantId\":12,\"tenantIdVer\":4,\"account\":12,\"avatar\":-4,\"code\":12,\"email\":12,\"errorCount\":4,\"errorTime\":93,\"jobsId\":12,\"jobsIdVer\":4,\"mobile\":12,\"password\":12,\"pwdModiTime\":93,\"realName\":12,\"sex\":12,\"status\":12},\"table\":\"sys_user\",\"ts\":1572932313298,\"type\":\"UPDATE\"}";
            sourceContext.collect(str);
            isRunning=false;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
