package com.elasticresthigh.createadd;

import org.apache.http.HttpHost;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pengfeisu
 * @create 2020-04-14 9:16
 */
public class ElasticClient {
    private RestHighLevelClient client;

    /**
     * Java High Level REST Client  初始化
     */
    public ElasticClient() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("bigdata1", 9200, "http")));
    }

    /**
     * 新增，修改文档
     *
     * @param indexName 索引
     * @param type      mapping type
     * @param id        文档id
     * @param jsonStr   文档数据
     */
    public void addData(String indexName, String type, String id, String jsonStr) {
        try {
            // 1、创建索引请求  //索引  // mapping type  //文档id
            IndexRequest request = new IndexRequest(indexName, type, id);     //文档id
            // 2、准备文档数据
            // 直接给JSON串
            request.source(jsonStr, XContentType.JSON);
            //4、发送请求
            IndexResponse indexResponse = null;
            try {
                // 同步方式
                indexResponse = client.index(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException e) {
                // 捕获，并处理异常
                //判断是否版本冲突、create但文档已存在冲突
                if (e.status() == RestStatus.CONFLICT) {
                    System.out.println("冲突了,请在此写冲突处理逻辑！" + e.getDetailedMessage());
                }
            }
            //5、处理响应
            if (indexResponse != null) {
                String index1 = indexResponse.getIndex();
                String type1 = indexResponse.getType();
                String id1 = indexResponse.getId();
                long version1 = indexResponse.getVersion();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("新增文档成功!" + index1 + type1 + id1 + version1);
                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("修改文档成功!");
                }
                // 分片处理信息
                ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    System.out.println("分片处理信息.....");
                }
                // 如果有分片副本失败，可以获得失败原因信息
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("副本失败原因：" + reason);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量插入ES
     *
     * @param indexName 索引
     * @param type      类型
     * @param idName    id名称
     * @param list      数据集合
     */
    public void bulkDate(String indexName, String type, String idName, List<Map<String, Object>> list) {
        try {

            if (null == list || list.size() <= 0) {
                return;
            }
            if (StringUtils.isBlank(indexName) || StringUtils.isBlank(idName) || StringUtils.isBlank(type)) {
                return;
            }
            BulkRequest request = new BulkRequest();
            for (Map<String, Object> map : list) {
                if (map.get(idName) != null) {
                    request.add(new IndexRequest(indexName, type, String.valueOf(map.get(idName)))
                            .source(map, XContentType.JSON));
                }
            }
            // 2、可选的设置
           /*
           request.timeout("2m");
           request.setRefreshPolicy("wait_for");
           request.waitForActiveShards(2);
           */
            //3、发送请求
            // 同步请求
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            //4、处理响应
            if (bulkResponse != null) {
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    DocWriteResponse itemResponse = bulkItemResponse.getResponse();

                    if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                            || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                        IndexResponse indexResponse = (IndexResponse) itemResponse;
                        //TODO 新增成功的处理
                        System.out.println("新增成功,{}" + indexResponse.toString());
                    } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                        UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                        //TODO 修改成功的处理
                        System.out.println("修改成功,{}" + updateResponse.toString());
                    } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                        DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                        //TODO 删除成功的处理
                        System.out.println("删除成功,{}" + deleteResponse.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String ags[]) {
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("id", "2");
        map1.put("user1", "bbherbert1");
        map1.put("postDate", "2018-08-30");
        map1.put("username", "aa");
        map1.put("message", "message");
        Map<String, Object> map2 = new HashMap<String, Object>();
        map2.put("id", "3");
        map2.put("user2", "bbherbert1");
        map2.put("postDate", "2018-08-30");
        map2.put("username", "aa");
        map2.put("message", "message");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("id", "1");
        map.put("user", "bbherbert1");
        map.put("postDate", "2018-08-30");
        map.put("username", "aa");
        map.put("message", "message");

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        list.add(map);
        list.add(map1);
        list.add(map2);
        ElasticClient esUtil = new ElasticClient();
        esUtil.bulkDate("book15", "boo", "id", list);
//        Map<String,Object> map = new HashMap<String, Object>();
//        map.put("user","herbert1");
//        map.put("postDate","2018-08-30");
//        map.put("username","aa");
//        map.put("message","message");
//        String jsonString = JSON.toJSONString(map);
//        esUtil.addData("hh","d","4",jsonString);
//        esUtil.addData("hh","d","4","{" +
//                "\"user\":\"kimchy\"," +
//                "\"postDate\":\"2013-01-30\"," +
//                "\"username\":\"zhangsan\"," +
//                "\"message\":\"trying out Elasticsearch\"" +
//                "}");
    }
}
