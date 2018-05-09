package com.gzfns.obdpps.util;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

public class IdGenerator {
    private static AtomicInteger inc = new AtomicInteger(1);
    
    static{
        inc = new AtomicInteger((int) (Math.random() * 10));
    }

    public static Long getBigId() {
        Long now = System.currentTimeMillis();
        Long result = Long.parseLong(now + getInc() + rand3Num());
        return result;
    }
    
    public static Integer getIntId(){
    	return Math.abs(getBigId().intValue());
    }
    private static String getInc() {
        if (inc.get() > 9999) {
            inc.set(1);
        }
        String incStr = inc.getAndIncrement() + "";
        while (incStr.length() < 4) {
            incStr = "0" + incStr;
        }
        return incStr;
    }

    private static int rand3Num() {
        int rand = (int) (Math.random() * 99);
        if (rand < 10) {
            rand = rand + 10;
        }
        return rand;
    }
    
    public static void main(String[] args) {
        for (int i = 0; i < 10000; i++) {
            System.out.println(BigDecimal.valueOf(Math.abs(getBigId().intValue())));
        }
    }
}
