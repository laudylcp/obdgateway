package com.gzfns.obdpps.command.vkt6;

import com.gzfns.obdpps.command.ICommand;

import java.util.Map;

public abstract class VKT6Command implements ICommand<String> {

    private String terminalId;

    private String header;

    private String functionType;

    private String functionItemKey;

    private String commandData;

    private String tail;

    public VKT6Command(){
        this.header = "*MG20";
        this.tail = "#";
    }

    @Override
    public String terminalId() {
        return terminalId;
    }

    @Override
    public Map<String, String> options() {
        return null;
    }

    @Override
    public String body() {
        return null;
    }

    public String getTerminalId() {
        return terminalId;
    }

    public void setTerminalId(String terminalId) {
        this.terminalId = terminalId;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getFunctionType() {
        return functionType;
    }

    public void setFunctionType(String functionType) {
        this.functionType = functionType;
    }

    public String getFunctionItemKey() {
        return functionItemKey;
    }

    public void setFunctionItemKey(String functionItemKey) {
        this.functionItemKey = functionItemKey;
    }

    public String getCommandData() {
        return commandData;
    }

    public void setCommandData(String commandData) {
        this.commandData = commandData;
    }

    public String getTail() {
        return tail;
    }

    public void setTail(String tail) {
        this.tail = tail;
    }
}
