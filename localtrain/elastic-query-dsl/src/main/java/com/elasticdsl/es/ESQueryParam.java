package com.elasticdsl.es;

import com.frameworkset.orm.annotation.ESMetaVersion;

/**
 * Created by player on 2020/3/17.
 */
public class ESQueryParam {

    private String sql;

    private String pageId;

    private String indexName = "mongo_message2020";

    private Integer page = 0;

    private Integer size = 20;

    //private String esUrl = ResourceUtils.getProperty("log.center.es.url");
    @ESMetaVersion
    private Long version;

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public static ESQueryParam build(SearchParam searchParam){
        ESQueryParam esQueryParam = new ESQueryParam();
        String essql = ESSQLBuilder.getESSQL(searchParam);
        esQueryParam.setSql(essql);
        //esQueryParam.setIndexName(ESSQLBuilder.getIndexName(searchParam));
        esQueryParam.setPage(searchParam.getPage());
        esQueryParam.setSize(searchParam.getSize());
        return esQueryParam;
    }

    public static ESQueryParam build(){
        return new ESQueryParam();
    }

    public ESQueryParam sql(String sql){
        this.sql = sql;
        return this;
    }
}
