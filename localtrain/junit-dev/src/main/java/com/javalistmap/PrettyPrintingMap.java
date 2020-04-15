package com.javalistmap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author pengfeisu
 * @create 2020-03-13 15:23
 */

public class PrettyPrintingMap<K, V> {

    private Map<K, V> map;

    public PrettyPrintingMap(Map<K, V> map) {
        this.map = map;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        return sb.toString();

    }

    public static void main(String[] args) {
        Map<String, String> myMap = new HashMap<String, String>();
        myMap.put("1001", "张三");
        myMap.put("1002", "李四");
        myMap.put("1003", "王五");
        System.out.println(new PrettyPrintingMap<String, String>(myMap));
    }
}
