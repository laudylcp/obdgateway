package com.gzfns.obdpps.command.vkt6;

public class DataPackageReplyMessage extends VKT6Command {

    @Override
    public String body() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getHeader());
        stringBuilder.append("Y");
        stringBuilder.append(getFunctionType());
        stringBuilder.append(getFunctionItemKey());
        stringBuilder.append(getTail());
        return stringBuilder.toString();
    }

}
