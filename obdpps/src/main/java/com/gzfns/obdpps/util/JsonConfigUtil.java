package com.gzfns.obdpps.util;

import io.vertx.core.json.JsonObject;

import java.io.InputStream;
import java.util.Scanner;

public class JsonConfigUtil {

    public static JsonObject getConfig() {
        return getConfig("config/dev/config-dev.json");
    }

    public static JsonObject getConfig(Class clazz) {
        return getConfig(clazz, "config/dev/config-dev.json");
    }

    public static JsonObject getConfig(String resourceName) {
        return getConfig(JsonConfigUtil.class,resourceName);
    }

    public static JsonObject getConfig(Class clazz, String resourceName) {
        InputStream resourceAsStream = clazz.getClassLoader().getResourceAsStream(resourceName);
        if (resourceAsStream == null) {
            throw new IllegalStateException("Cannot find " + resourceName + " on classpath");
        }
        try {
            Scanner scanner = (new Scanner(resourceAsStream, "UTF-8")).useDelimiter("\\A");
            String scanString = scanner.hasNext() ? scanner.next() : "";
            return new JsonObject(scanString);
        } catch (Exception e) {
            System.out.println("Failed get config from: " + resourceName);
            e.printStackTrace();
        }
        return null;
    }
}
