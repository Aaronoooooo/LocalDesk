package com.elasticdsl.es;

import java.util.*;

/**
 * Created by player on 2019/12/12.
 */
public class ES6QueryImpl extends ESQuery {

    public ES6QueryImpl() {
        super("/_xpack/sql/translate", "/_search");
    }

    @Override
    protected List<Map> parseESQueryResultJson(Map queryResultMap) {

        List<Map> result = new ArrayList<>();
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
        return result;
    }
}
