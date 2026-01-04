package com.nan.kvstore.gateway;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class ConsistentHashRouter {

    private final List<String> nodes;

    public ConsistentHashRouter(List<String> nodes) {
        this.nodes = nodes;
    }

    // For RF=3, return: primary + next 2 nodes (circular)
    public List<String> pickReplicaNodes(String key, int rf) {
        int primaryIdx = Math.floorMod(hashToInt(key), nodes.size());

        List<String> replicas = new ArrayList<>();
        for (int i = 0; i < rf; i++) {
            int idx = (primaryIdx + i) % nodes.size();
            replicas.add(nodes.get(idx));
        }
        return replicas;
    }

    public String pickPrimaryNode(String key) {
        return pickReplicaNodes(key, 1).get(0);
    }

    private int hashToInt(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            int val = ((digest[0] & 0xff) << 24)
                    | ((digest[1] & 0xff) << 16)
                    | ((digest[2] & 0xff) << 8)
                    | (digest[3] & 0xff);
            return val;
        } catch (Exception e) {
            return key.hashCode();
        }
    }
}