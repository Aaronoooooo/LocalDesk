package com.elasticdsl.es;

public class ChatHistoryEntity {
    private String senderId;
    private String recipientId;
    private String fanId;
    private String fanName;
    private String pageId;
    private String lastShowMsg;
    private String mid;
    private String timestamp;
    private String click;
    private String read;
    private String delivery;
    private String read_timestamp;
    private String delivery_timestamp;
    private String topicId;
    private String otnType;
    private String userId;
    private String broadcastId;
    private String flowNodeUuid;
    private String flowId;
    private String workflowId;

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public String getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(String recipientId) {
        this.recipientId = recipientId;
    }

    public String getFanId() {
        return fanId;
    }

    public void setFanId(String fanId) {
        this.fanId = fanId;
    }

    public String getFanName() {
        return fanName;
    }

    public void setFanName(String fanName) {
        this.fanName = fanName;
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

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getClick() {
        return click;
    }

    public void setClick(String click) {
        this.click = click;
    }

    public String getRead() {
        return read;
    }

    public void setRead(String read) {
        this.read = read;
    }

    public String getDelivery() {
        return delivery;
    }

    public void setDelivery(String delivery) {
        this.delivery = delivery;
    }

    public String getRead_timestamp() {
        return read_timestamp;
    }

    public void setRead_timestamp(String read_timestamp) {
        this.read_timestamp = read_timestamp;
    }

    public String getDelivery_timestamp() {
        return delivery_timestamp;
    }

    public void setDelivery_timestamp(String delivery_timestamp) {
        this.delivery_timestamp = delivery_timestamp;
    }

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    public String getOtnType() {
        return otnType;
    }

    public void setOtnType(String otnType) {
        this.otnType = otnType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getBroadcastId() {
        return broadcastId;
    }

    public void setBroadcastId(String broadcastId) {
        this.broadcastId = broadcastId;
    }

    public String getFlowNodeUuid() {
        return flowNodeUuid;
    }

    public void setFlowNodeUuid(String flowNodeUuid) {
        this.flowNodeUuid = flowNodeUuid;
    }

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    @Override
    public String toString() {
        return "ChatHistoryEntity{" +
                "senderId='" + senderId + '\'' +
                ", recipientId='" + recipientId + '\'' +
                ", fanId='" + fanId + '\'' +
                ", fanName='" + fanName + '\'' +
                ", pageId='" + pageId + '\'' +
                ", lastShowMsg='" + lastShowMsg + '\'' +
                ", mid='" + mid + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", click='" + click + '\'' +
                ", read='" + read + '\'' +
                ", delivery='" + delivery + '\'' +
                ", read_timestamp='" + read_timestamp + '\'' +
                ", delivery_timestamp='" + delivery_timestamp + '\'' +
                ", topicId='" + topicId + '\'' +
                ", otnType='" + otnType + '\'' +
                ", userId='" + userId + '\'' +
                ", broadcastId='" + broadcastId + '\'' +
                ", flowNodeUuid='" + flowNodeUuid + '\'' +
                ", flowId='" + flowId + '\'' +
                ", workflowId='" + workflowId + '\'' +
                '}';
    }
}
