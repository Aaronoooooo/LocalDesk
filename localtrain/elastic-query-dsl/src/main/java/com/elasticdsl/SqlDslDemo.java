package com.elasticdsl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;

/**
 * @author pengfeisu
 * @create 2020-04-13 15:24
 */
public class SqlDslDemo {
    public static void main(String[] args) {

        ClientInterface clientUtil = ElasticSearchHelper.getRestClientUtil();
        String json = clientUtil.executeHttp("/_sql/translate",
                "{\"query\": \"SELECT * FROM czting_console_log_test limit 10\"}",
                ClientInterface.HTTP_POST
        );
        System.out.println(json);

        String querResult = clientUtil.executeHttp("/czting_console_log_test/_search", json,
                ClientInterface.HTTP_POST
        );
        System.out.println(querResult);
        JSONObject jsonObject = JSONObject.parseObject(querResult);
        String hits = jsonObject.getString("hits");
        JSONObject object = JSONObject.parseObject(hits);
        String hits1 = MapUtils.getString(object, "hits");
        JSONArray objects = JSONArray.parseArray(hits1);
        for (int i = 0; i < objects.size(); i++) {
            String fields = objects.getJSONObject(i).getString("fields");
            System.out.println(fields);
        }
        System.out.println(hits1);

    }
}
