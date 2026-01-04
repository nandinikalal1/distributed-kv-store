package com.nan.kvstore.service;

import org.springframework.stereotype.Service;

import com.nan.kvstore.cache.LruCache;
import com.nan.kvstore.model.VersionedValue;
import com.nan.kvstore.store.InMemoryKeyValueStore;

/*
  Service now manages VersionedValue.
  Version is generated when client doesn't provide one.
*/
@Service
public class KvService {

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore();
    private final LruCache cache = new LruCache(100);

    public VersionedValue get(String key) {
        VersionedValue fromCache = cache.get(key);
        if (fromCache != null) return fromCache;

        VersionedValue fromStore = store.get(key);
        if (fromStore != null) cache.put(key, fromStore);

        return fromStore;
    }

    // If version is provided, store it. If not, generate one.
    public VersionedValue put(String key, String value, Long versionOpt) {
        long version = (versionOpt != null) ? versionOpt : System.currentTimeMillis();

        VersionedValue vv = new VersionedValue(value, version);

        store.put(key, vv);
        cache.put(key, vv);

        return vv;
    }

    public boolean delete(String key) {
        boolean existed = store.containsKey(key);
        store.delete(key);
        cache.remove(key);
        return existed;
    }
}
