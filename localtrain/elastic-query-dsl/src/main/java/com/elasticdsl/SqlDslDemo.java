package com.elasticdsl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.elasticdsl.es.JsonUtil;
import org.apache.commons.collections.MapUtils;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;

import java.util.*;

/**
 * @author pengfeisu
 * @create 2020-04-13 15:24
 */
public class SqlDslDemo {
    public static void main(String[] args) {

        ClientInterface clientUtil = ElasticSearchHelper.getRestClientUtil();
        String json = clientUtil.executeHttp("/_xpack/sql/translate",
//        String json = clientUtil.executeHttp("/_xpack/sql?format=json",
                "{\"query\": \"SELECT * FROM mongo_message2020 where pageId='108239937582293' and lastShowMsg like '%球%'\"}",
                ClientInterface.HTTP_POST
        );
        Map translateMap = JsonUtil.getMapByJson(json);
        if (translateMap.containsKey("error")) {
            throw new RuntimeException("执行ES翻译时出错:" + json);
        }
        System.out.println("********************打印dsl语句*********************");
        System.out.println(json);



        String querResult = clientUtil.executeHttp("/mongo_message2020/_search", json,
                ClientInterface.HTTP_POST
        );
        //*****************************custom-start***************************
        List<Map> result = new ArrayList<>();
        Map queryResultMap = JsonUtil.getMapByJson(querResult);
        if (queryResultMap.containsKey("error")) {
            throw new RuntimeException("执行ES查询时出错:" + queryResultMap);
        } else if (queryResultMap.containsKey("hits")) {
            Map hitsMap = (Map) (queryResultMap.get("hits"));
            Long total = org.apache.commons.collections4.MapUtils.getLong(hitsMap, "total");
            List<Map> hits = (List<Map>) hitsMap.get("hits");
            for (Map hit : hits) {
                Map fields = (Map) hit.get("fields");
                Set<Map.Entry> set = fields.entrySet();
                Map _source = (Map) hit.get("_source");
                if (_source == null || _source.isEmpty()) {
                    _source = new LinkedHashMap();
                }
                for (Map.Entry entry : set) {
                    String key = (String) entry.getKey();
                    List entryValue = (List) entry.getValue();
                    _source.put(key, entryValue.get(0));
                }
                result.add(_source);
            }
        }
        System.out.println("查询结果集:" + result);
        //*****************************custom-end***************************
        //System.out.print("查询结果:" + querResult);
        JSONObject jsonObject = JSONObject.parseObject(querResult);
        String hits = jsonObject.getString("hits");
        JSONObject object = JSONObject.parseObject(hits);
        String hits1 = MapUtils.getString(object, "hits");
        JSONArray objects = JSONArray.parseArray(hits1);
        for (int i = 0; i < objects.size(); i++) {
            String fields = objects.getJSONObject(i).getString("fields");
//            System.out.println("获取dsl中的fields:" + fields);
        }
//        System.out.println("获取dsl中的hits:" + hits1);

    }

}
