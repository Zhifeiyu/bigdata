package com.zfylin.demo.bigdata.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 配置文件读取工具
 * <p>
 * Created by zfy on 2016/9/18.
 */
public class Properties {
    private static Map<String, java.util.Properties> propertieses = new ConcurrentHashMap(); // 配置文件map，线程安全
    private java.util.Properties properties; // 当前使用的配置文件

    private static java.util.Properties getProperties(String fileName) {
        synchronized (propertieses) {
            java.util.Properties properties = propertieses.get(fileName);
            if (properties == null) {
                // 默认配置文件目录为src/main/resources目录
                InputStream fileStream = Properties.class.getClassLoader().getResourceAsStream(fileName);
                properties = new java.util.Properties();
                try {
                    properties.load(fileStream);
                } catch (IOException e) {
                    e.printStackTrace();
                    try {
                        fileStream.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                } finally {
                    try {
                        fileStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                propertieses.put(fileName, properties);
            }
            return properties;
        }
    }

    public Properties(String fileName) {
        properties = getProperties(fileName);
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return value != null ? value : defaultValue;
    }

    public int getPropertyInteger(String key) {
        String value = this.properties.getProperty(key);
        if (value != null)
            return Integer.parseInt(value);
        return 0;
    }

    public int getPropertyInteger(String key, int defaultValue) {
        String value = this.properties.getProperty(key);
        if (value != null)
            return Integer.parseInt(value);
        return defaultValue;
    }

    public boolean getPropertyBoolean(String key) {
        if (StringUtils.isEmpty(key)) {
            return false;
        }
        String value = getProperty(key);
        if (value == null)
            return false;
        return (value.equals("1")) || (value.equals("true"));
    }

    public boolean getPropertyBoolean(String key, boolean defaultValue) {
        if (StringUtils.isEmpty(key)) {
            return defaultValue;
        }
        String value = getProperty(key);
        if (value == null)
            return defaultValue;
        return (value.equals("1")) || (value.equals("true"));
    }

    public static void main(String[] args) {
        Properties properties = new Properties("common.properties");
        System.out.println(properties.getProperty("key01"));
        System.out.println(properties.getPropertyInteger("key02"));
        System.out.println(properties.getPropertyBoolean("key03"));
        System.out.println(properties.getPropertyBoolean("key04"));
        System.out.println(properties.getPropertyBoolean("key05"));
        System.out.println(properties.getPropertyBoolean("key06"));
    }
}
