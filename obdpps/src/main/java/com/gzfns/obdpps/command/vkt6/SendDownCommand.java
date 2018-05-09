package com.gzfns.obdpps.command.vkt6;

import org.apache.commons.lang3.StringUtils;

public class SendDownCommand extends VKT6Command {

    private String storeProperty;

    private String replyProperty;

    private String commandBody;

    @Override
    public String body() {
        if(!StringUtils.isBlank(commandBody)){
            return commandBody;
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getHeader());
        stringBuilder.append(storeProperty);
        stringBuilder.append(replyProperty);
        stringBuilder.append(getFunctionType());
        stringBuilder.append(getFunctionItemKey());
        stringBuilder.append(getCommandData());
        stringBuilder.append(getTail());
        return stringBuilder.toString();
    }

    public String getStoreProperty() {
        return storeProperty;
    }

    public void setStoreProperty(String storeProperty) {
        this.storeProperty = storeProperty;
    }

    public String getReplyProperty() {
        return replyProperty;
    }

    public void setReplyProperty(String replyProperty) {
        this.replyProperty = replyProperty;
    }

    public String getCommandBody() {
        return commandBody;
    }

    public void setCommandBody(String commandBody) {
        this.commandBody = commandBody;
    }
}
