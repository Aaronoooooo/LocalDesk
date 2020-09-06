package com.elasticdsl.es;

import com.frameworkset.orm.annotation.ESMetaVersion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by player on 2019/12/12.
 */
public class ESQueryUtil{
    @ESMetaVersion
    private long version;

    private static Map<String, ESQuery> esQueryMap = new HashMap<>();

    static {
        //esQueryMap.put("7", new ES7QueryImpl());
        esQueryMap.put("6", new ES6QueryImpl());
    }

    //执行ES查询
    public static List<Map> executeESQuery(ESQueryParam esQueryParam) {
        //String esUrl = esQueryParam.getEsUrl();
        //if (StringUtils.isEmpty(esUrl)) {
        //    throw new RuntimeException("ES-URL不能为空!");
        //}


        ESQuery esQuery = null;
        esQuery = esQueryMap.get("6");
        return esQuery.query(esQueryParam);
    }
}

