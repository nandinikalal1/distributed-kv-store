package com.nan.kvstore.store;

import java.util.concurrent.ConcurrentHashMap;

import com.nan.kvstore.model.VersionedValue;

/*
  Store now keeps VersionedValue for each key instead of plain String.
*/
public class InMemoryKeyValueStore {

    private final ConcurrentHashMap<String, VersionedValue> map = new ConcurrentHashMap<>();

    public VersionedValue get(String key) {
        return map.get(key);
    }

    public void put(String key, VersionedValue value) {
        map.put(key, value);
    }

    public void delete(String key) {
        map.remove(key);
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }
}
