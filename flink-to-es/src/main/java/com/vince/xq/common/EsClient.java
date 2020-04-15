package com.vince.xq.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;

/**
 * @author
 * @Date 2019-11-13 15:56
 **/
public class EsClient {
    private static volatile RestClient client;

    private EsClient() {
    }

    public static RestClient getInstance(String url, int port) throws UnknownHostException {
        if (client == null) {
            synchronized (EsClient.class) {
                if (client == null) {
                    client = RestClient
                            .builder(new HttpHost(url, port, "http"))
                            .build();
                    return client;
                }
            }
        }
        return client;
    }

    public static JSONObject getSource(RestClient client, String queryStr, String index, String type) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new NStringEntity(queryStr, ContentType.APPLICATION_JSON);
        String search = "/" + index + "/" + type + "/_search";
        Response response = client.performRequest("GET", search, params, entity);
        String responseBody = null;
        responseBody = EntityUtils.toString(response.getEntity());
        System.out.println(response.getStatusLine().getStatusCode());
        if (response.getStatusLine().getStatusCode() == 200) {
            JSONObject jsonObject = JSONObject.parseObject(responseBody);
            JSONObject object = JSONObject.parseObject(jsonObject.get("hits").toString());
            System.out.println(object.getString("hits"));
            JSONArray jsonArray = JSONObject.parseArray(object.getString("hits"));
            if (jsonArray.size() == 0) {
                System.out.println("null");
                return null;
            }
            JSONObject source = JSONObject.parseObject(JSONObject.parseObject(jsonArray.get(0).toString()).getString("_source"));
            return source;
        } else {
            return null;
        }
    }
}
