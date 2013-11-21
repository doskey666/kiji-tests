package com.data.bt.utils;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: acohen
 * Date: 11/21/13
 * Time: 2:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class MapUtils {
    public static <K,V> V putIfAbsent(Map<K, V> destinationMap, K currentKey, V value) {
        V foundValue = destinationMap.get(currentKey);
        if (foundValue == null) {
            destinationMap.put(currentKey, value);
            return value;
        }

        return foundValue;
    }
}
