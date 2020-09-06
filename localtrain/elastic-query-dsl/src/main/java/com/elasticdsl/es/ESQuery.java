package com.elasticdsl.es;

import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by player on 2019/12/12.
 */
public abstract class ESQuery {

    protected String translatePath;

    protected String searchPath;

    public List<Map> query(ESQueryParam esQueryParam) {

        //String pageId = esQueryParam.getPageId();

        //StringBuilder stringBuilder = new StringBuilder();
        //stringBuilder.append("select * from " + indexName + " where 1=1");
        //stringBuilder.append(" and");
        //stringBuilder.append(" pageId=" + pageId);
        //stringBuilder.append(" and");
        //stringBuilder.append(" lastShowMsg like '%球%'");
        //stringBuilder.append(" order by watermark desc");
        //map.put("query", stringBuilder.toString());

        String indexName = esQueryParam.getIndexName();
        String sql = esQueryParam.getSql();
        Map map = new HashMap();

        map.put("query",sql);
        //执行/_xpack/sql/translateh 获取dsl语句
        ClientInterface clientUtil = ElasticSearchHelper.getRestClientUtil();
        String translateJson = clientUtil.executeHttp(translatePath,
                JsonUtil.toJson(map),
                ClientInterface.HTTP_POST);
        //处理时间条件问题,因为logDateTime会有空,而且console跟action两边类型还不一样
        translateJson = translateJson.replace("{\"range\":{\"logDateTime\"","{\"range\":{\"@timestamp\"");
        //处理排序问题,因为logDateTime有可能会有空的数据导致排在前面
        translateJson = translateJson.replace("\"sort\":[{\"logDateTime\"","\"sort\":[{\"@timestamp\"");
        Map translateMap = JsonUtil.getMapByJson(translateJson);
        if (translateMap.containsKey("error")) {
            throw new RuntimeException("执行ES翻译时出错:" + translateJson);
        }

        //分页处理
        Integer page = esQueryParam.getPage();
        int size = esQueryParam.getSize();
        translateMap.put("from", page * size);
        translateMap.put("size", size);
        translateMap.put("track_total_hits", true);//设置该值为了正确返回total数
        translateJson = JsonUtil.toJson(translateMap);

        //执行/_xpack/sql/translateh结果语句
        String queryResultJson = clientUtil.executeHttp("/" + indexName + "/_search", translateJson,
                ClientInterface.HTTP_POST
        );
        Map queryResultMap = JsonUtil.getMapByJson(queryResultJson);
        if (queryResultMap.containsKey("error")) {
            throw new RuntimeException("执行ES查询时出错:" + queryResultJson);
        }

        List<Map> listResponseInfo = parseESQueryResultJson(queryResultMap);
        //listResponseInfo.setMessage(sql);
        return listResponseInfo;
    }

    protected abstract List<Map> parseESQueryResultJson(Map resultMap);

    public ESQuery(String translatePath, String searchPath) {
        this.translatePath = translatePath;
        this.searchPath = searchPath;
    }
}
