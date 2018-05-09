package com.gzfns.obdpps.command;

import java.util.Map;

public interface ICommand<T> {

    String terminalId();

    Map<String, String> options();

    T body();

}
