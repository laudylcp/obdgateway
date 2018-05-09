package com.gzfns.obdpps.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

public class DeviceCommandEntity implements Serializable {

    @JsonProperty(value = "Id")
    private Long id;

    @JsonProperty(value = "Imei")
    private String imei;

    @JsonProperty(value = "CommandType")
    private String commandType;

    @JsonProperty(value = "CommandContent")
    private String commandContent;

    @JsonProperty(value = "AddTime")
    private Date addTime;

    @JsonProperty(value = "SendTime")
    private Date sendTime;

    @JsonProperty(value = "IsSend")
    private Boolean isSend;

    @JsonProperty(value = "IsReply")
    private Boolean isReply;

    @JsonProperty(value = "ReplyTime")
    private Date replyTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getCommandType() {
        return commandType;
    }

    public void setCommandType(String commandType) {
        this.commandType = commandType;
    }

    public String getCommandContent() {
        return commandContent;
    }

    public void setCommandContent(String commandContent) {
        this.commandContent = commandContent;
    }

    public Date getAddTime() {
        return addTime;
    }

    public void setAddTime(Date addTime) {
        this.addTime = addTime;
    }

    public Date getSendTime() {
        return sendTime;
    }

    public void setSendTime(Date sendTime) {
        this.sendTime = sendTime;
    }

    public Boolean getIsSend() {
        return isSend;
    }

    public void setIsSend(Boolean send) {
        isSend = send;
    }

    public Boolean getIsReply() {
        return isReply;
    }

    public void setIsReply(Boolean reply) {
        isReply = reply;
    }

    public Date getReplyTime() {
        return replyTime;
    }

    public void setReplyTime(Date replyTime) {
        this.replyTime = replyTime;
    }
}
