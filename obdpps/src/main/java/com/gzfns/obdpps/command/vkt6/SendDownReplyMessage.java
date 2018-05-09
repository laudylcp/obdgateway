package com.gzfns.obdpps.command.vkt6;

public class SendDownReplyMessage extends VKT6Command {

    private String replyProperty;

    @Override
    public String body() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getHeader());
        stringBuilder.append(replyProperty);
        stringBuilder.append(getTerminalId());
        stringBuilder.append(",");
        stringBuilder.append("Y");
        stringBuilder.append(getFunctionType());
        stringBuilder.append(getFunctionItemKey());
        stringBuilder.append(getTail());
        return stringBuilder.toString();
    }

    public String getReplyProperty() {
        return replyProperty;
    }

    public void setReplyProperty(String replyProperty) {
        this.replyProperty = replyProperty;
    }
}
