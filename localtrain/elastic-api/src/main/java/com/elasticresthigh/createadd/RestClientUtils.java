package com.elasticresthigh.createadd;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @author pengfeisu
 * @create 2020-03-07 20:50
 */

public class RestClientUtils {

    /**
     * 高阶Rest Client
     */
    private RestHighLevelClient client = null;
    /**
     * 低阶Rest Client
     */
    private RestClient restClient = null;

    /**
     * 这里使用饿汉单例模式创建RestHighLevelClient
     */
    public RestClientUtils() {
        if (client == null) {
            synchronized (RestHighLevelClient.class) {
                if (client == null) {
                    client = getClient();
                }
            }
        }
    }

    /*RestHighLevelClient*/
    private RestHighLevelClient getClient() {
        RestHighLevelClient client = null;

        try {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("bigdata1", 9300, "http")
                    )
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    /*RestClient*/
    private RestClient getRestClient() {
        RestClient client = null;

        try {
            client = RestClient.builder(
                    new HttpHost("bigdata1", 9300, "http")
            ).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    public void closeClient() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        RestClientUtils restClientUtils = new RestClientUtils();
        restClientUtils.index();
    }

    /**
     * document API 主要是些简单的增删改查操作
     */
    /*增、插入记录;插入操作有四种方式:分同步异步操作,可选参数设置,结果返回IndexResponse,抛出异常@throws Exception*/
    public void index() {

        /*第一种方式:String*/
        IndexRequest request = new IndexRequest("posts", "doc", "2");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2020-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

    }

    /**
     * Search API 主要是些复杂查询操作
     */
    public void searchAPI() {
        //...
    }
}
