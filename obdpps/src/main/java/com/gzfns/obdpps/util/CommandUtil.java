package com.gzfns.obdpps.util;

public class CommandUtil {

    public static String get4BytesHexString(int i) {
        String hexString = Integer.toHexString(i).toUpperCase();

        int len = hexString.length();
        if(len == 1) {
            hexString = "000" + hexString;
        }
        else if(len == 2) {
            hexString = "00" + hexString;
        }
        else if(len == 3) {
            hexString = "0" + hexString;
        }

        if(len > 4) {
            hexString = hexString.substring(len - 4);
        }

        return hexString;
    }

}
