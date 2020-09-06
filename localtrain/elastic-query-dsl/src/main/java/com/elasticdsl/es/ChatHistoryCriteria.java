package com.elasticdsl.es;

import java.io.Serializable;

public class ChatHistoryCriteria extends BaseCriteria<ChatHistoryEntity> implements Serializable {

    private String processingTime; //ES入库时间@timestamp
    private String pageId;
    private String lastShowMsg;

    public String getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(String processingTime) {
        this.processingTime = processingTime;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public String getLastShowMsg() {
        return lastShowMsg;
    }

    public void setLastShowMsg(String lastShowMsg) {
        this.lastShowMsg = lastShowMsg;
    }
}
