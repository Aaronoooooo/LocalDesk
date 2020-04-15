package com.vince.xq.mapper;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.vince.xq.common.EsClient;
import com.vince.xq.model.LogModel;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.elasticsearch.client.RestClient;

/**
 * @author
 * @Date 2019-11-13 15:51
 **/
public class LogMapper extends RichFlatMapFunction<String, String> {
    private RestClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("123");
        ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
        ParameterTool params = (ParameterTool) executionConfig.getGlobalJobParameters();
        String url = params.getProperties().getProperty("es.http.url");
        int port =Integer.parseInt(params.getProperties().getProperty("es.http.port"));
        System.out.println(url);
        System.out.println(port);
        client = EsClient.getInstance(url, port);
    }

    @Override
    public void flatMap(String value, Collector<String> collector) throws Exception {
        System.out.println(value);
        JSONObject jsonObject = JSONObject.parseObject(value);
        JSONArray jsonArray = JSONArray.parseArray(jsonObject.get("data").toString());
        JSONObject object = JSONObject.parseObject(jsonArray.get(0).toString());
        System.out.println(object.getString("id"));
        System.out.println(object.getString("orgId"));
        System.out.println(object.getString("creator"));

        String userName = "";
        String userId = object.getString("creator");
        String queryString = "{\"query\":{\"match\":{\"id\":\"" + userId + "\"}}}";
        JSONObject userObject = EsClient.getSource(client, queryString, "sys_user_test", "user");
        if (userObject != null) {
            userName = userObject.getString("realName");
        }

        String orgName = "";
        String orgId = object.getString("orgId");
        String queryOrgString = "{\"query\":{\"match\":{\"orgId\":\"" + orgId + "\"}}}";
        JSONObject orgObject = EsClient.getSource(client, queryOrgString, "sys_org_test", "org");
        if (orgObject != null) {
            orgName = orgObject.getString("name");
        }

        LogModel logModel = new LogModel();
        logModel.setId(object.getString("id"));
        logModel.setUserId(object.getString("creator"));
        logModel.setUserName(userName);
        logModel.setOrgId(object.getString("orgId"));
        logModel.setOrgName(orgName);
        System.out.println("--------------"+logModel);
        collector.collect(JSONObject.toJSONString(logModel));
    }
}
