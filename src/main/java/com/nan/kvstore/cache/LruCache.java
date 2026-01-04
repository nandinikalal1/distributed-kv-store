package com.nan.kvstore.cache;

import java.util.LinkedHashMap;
import java.util.Map;

import com.nan.kvstore.model.VersionedValue;

public class LruCache {

    private final int capacity;
    private final Map<String, VersionedValue> cache;

    public LruCache(int capacity) {
        this.capacity = capacity;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, VersionedValue> eldest) {
                return size() > LruCache.this.capacity;
            }
        };
    }

    public synchronized VersionedValue get(String key) {
        return cache.get(key);
    }

    public synchronized void put(String key, VersionedValue value) {
        cache.put(key, value);
    }

    public synchronized void remove(String key) {
        cache.remove(key);
    }
}
