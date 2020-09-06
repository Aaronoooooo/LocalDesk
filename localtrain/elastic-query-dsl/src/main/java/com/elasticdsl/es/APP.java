package com.elasticdsl.es;

public class APP {
    public static void main(String[] args) {
        //ESQueryParam esQueryParam = new ESQueryParam();
        //esQueryParam.setPageId("108239937582293");
        //esQueryParam.setIndexName("mongo_message2020");

        SearchParam<ChatHistoryCriteria> searchParam = new SearchParam<>();
        //searchParam.setCriteria();
        ChatHistoryCriteria criteria = searchParam.getCriteria();
        criteria.setPageId("108239937582293");
        criteria.setLastShowMsg("球");
        ESQueryParam esQueryParam = ESQueryParam.build(searchParam);
        ESQueryUtil.executeESQuery(esQueryParam);
    }
}
