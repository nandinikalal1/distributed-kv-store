package com.nan.kvstore.store;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKeyValueStore {
/**
 * It stores key-value data in RAM using a thread-safe map.
 * Why ConcurrentHashMap?
 * - Your server can handle multiple HTTP requests at the same time (concurrent requests).
 * - Thread-safe map prevents data corruption.
 */


    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    // Returns the value for a key, or null if the key does not exist.
    public String get(String key) {
        return map.get(key);
    }

    // Saves or overwrites the value for the key.
    public void put(String key, String value) {
        map.put(key, value);
    }

    // Deletes the key (does nothing if key doesn't exist).
    public void delete(String key) {
        map.remove(key);
    }

    // Used to check existence (helpful for returning 404 on delete).
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }
}
