package com.nan.kvstore.gateway;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.nan.kvstore.model.VersionedValue;

@RestController
@RequestMapping("/gkv")
public class GatewayController {

    private final RestTemplate restTemplate = new RestTemplate();

    // All nodes in cluster
    private final List<String> allNodes = List.of(
            "http://localhost:8081",
            "http://localhost:8082",
            "http://localhost:8083"
    );

    private final ConsistentHashRouter router = new ConsistentHashRouter(allNodes);

    // Quorum config (defaults)
    private static final int N = 3;
    private static final int DEFAULT_W = 2;
    private static final int DEFAULT_R = 2;

    // Just for debugging: helps you confirm you're hitting the NEW gateway
    private static final String GATEWAY_BUILD = "GATEWAY_6B_HEALTH_AWARE_v1";

    /*
      Health table maintained by gateway:
      nodeHealth[nodeUrl] = true/false
    */
    private final Map<String, Boolean> nodeHealth = new ConcurrentHashMap<>();

    public GatewayController() {
        // initialize health to true so startup isn't blocked
        for (String n : allNodes) {
            nodeHealth.put(n, true);
        }

        System.out.println("[Gateway] Started " + GATEWAY_BUILD + " at " + Instant.now());
        System.out.println("[Gateway] Nodes=" + allNodes);

        // Background thread: ping nodes every 2 seconds
        Thread healthThread = new Thread(this::healthLoop);
        healthThread.setDaemon(true);
        healthThread.start();
    }

    // A quick endpoint to confirm you are hitting THIS gateway instance
    // GET /gkv/whoami
    @GetMapping("/whoami")
    public ResponseEntity<String> whoami() {
        return ResponseEntity.ok(GATEWAY_BUILD);
    }

    // ------------------ HEALTH CHECK LOOP ------------------
    private void healthLoop() {
        while (true) {
            for (String node : allNodes) {
                nodeHealth.put(node, isNodeUp(node));
            }

            try {
                Thread.sleep(2000); // ping every 2s
            } catch (InterruptedException ignored) {
            }
        }
    }

    private boolean isNodeUp(String nodeBaseUrl) {
        try {
            String url = nodeBaseUrl + "/kv/health";

            // We only care about HTTP 200. We don't need JSON parsing here.
            ResponseEntity<String> resp = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            return resp.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            return false;
        }
    }

    // For debugging: see which nodes are currently UP/DOWN
    // GET /gkv/cluster/health
    @GetMapping("/cluster/health")
    public ResponseEntity<Map<String, Boolean>> clusterHealth() {
        // ensure no nulls
        Map<String, Boolean> snapshot = new LinkedHashMap<>();
        for (String n : allNodes) {
            snapshot.put(n, Boolean.TRUE.equals(nodeHealth.get(n)));
        }
        return ResponseEntity.ok(snapshot);
    }

    // Pick replica list for key, filtered by health
    private List<String> healthyReplicasForKey(String key) {
        List<String> replicas = router.pickReplicaNodes(key, N);
        List<String> healthy = new ArrayList<>();
        for (String r : replicas) {
            if (Boolean.TRUE.equals(nodeHealth.get(r))) {
                healthy.add(r);
            }
        }
        return healthy;
    }

    // ------------------ PUT (health-aware quorum write) ------------------
    @PutMapping("/put")
    public ResponseEntity<String> put(@RequestParam String key,
                                      @RequestParam String value,
                                      @RequestParam(defaultValue = "" + DEFAULT_W) int w) {

        if (w < 1) w = 1;
        if (w > N) w = N;

        List<String> healthyReplicas = healthyReplicasForKey(key);

        // Fail fast if quorum is impossible
        if (healthyReplicas.size() < w) {
            return ResponseEntity.status(503).body(
                    "WRITE FAILED FAST. Not enough healthy replicas for w=" + w +
                            ". Healthy=" + healthyReplicas + " HealthTable=" + clusterHealth().getBody()
            );
        }

        long version = System.currentTimeMillis();

        List<String> successes = new ArrayList<>();
        List<String> failures = new ArrayList<>();

        // Try only healthy replicas; stop once quorum is met
        for (String node : healthyReplicas) {
            if (successes.size() >= w) break;

            String url = node + "/kv/put?key=" + key + "&value=" + value + "&version=" + version;
            try {
                restTemplate.exchange(url, HttpMethod.PUT, null, VersionedValue.class);
                successes.add(node);
            } catch (RestClientException e) {
                failures.add(node);
            }
        }

        if (successes.size() < w) {
            return ResponseEntity.status(500).body(
                    "WRITE FAILED (need w=" + w + "). version=" + version +
                            " Success=" + successes + " Fail=" + failures
            );
        }

        return ResponseEntity.ok(
                "WRITE QUORUM OK (w=" + w + "). version=" + version +
                        " Success=" + successes +
                        (failures.isEmpty() ? "" : " Fail=" + failures) +
                        " HealthyCandidates=" + healthyReplicas
        );
    }

    // ------------------ GET (health-aware quorum read) ------------------
    @GetMapping("/get")
    public ResponseEntity<String> get(@RequestParam String key,
                                      @RequestParam(defaultValue = "" + DEFAULT_R) int r,
                                      @RequestParam(defaultValue = "true") boolean repair) {

        if (r < 1) r = 1;
        if (r > N) r = N;

        List<String> healthyReplicas = healthyReplicasForKey(key);

        // Fail fast if quorum is impossible
        if (healthyReplicas.size() < r) {
            return ResponseEntity.status(503).body(
                    "READ FAILED FAST. Not enough healthy replicas for r=" + r +
                            ". Healthy=" + healthyReplicas + " HealthTable=" + clusterHealth().getBody()
            );
        }

        List<ReplicaRead> reads = new ArrayList<>();

        // Collect r successful reads
        for (String node : healthyReplicas) {
            if (reads.size() >= r) break;

            String url = node + "/kv/get?key=" + key;
            try {
                ResponseEntity<VersionedValue> resp =
                        restTemplate.exchange(url, HttpMethod.GET, null, VersionedValue.class);

                if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                    reads.add(new ReplicaRead(node, resp.getBody()));
                }
            } catch (RestClientException ignored) {
            }
        }

        if (reads.size() < r) {
            return ResponseEntity.status(500).body(
                    "READ FAILED (need r=" + r + "). Only got " + reads.size() +
                            " SuccessfulReadsFrom=" + reads
            );
        }

        ReplicaRead newest = reads.stream()
                .max(Comparator.comparingLong(rr -> rr.value.getVersion()))
                .get();

        // Read repair (only among replicas we successfully read)
        if (repair) {
            for (ReplicaRead rr : reads) {
                if (rr.value.getVersion() < newest.value.getVersion()) {
                    String repairUrl = rr.node + "/kv/put?key=" + key
                            + "&value=" + newest.value.getValue()
                            + "&version=" + newest.value.getVersion();
                    try {
                        restTemplate.exchange(repairUrl, HttpMethod.PUT, null, VersionedValue.class);
                    } catch (RestClientException ignored) {
                    }
                }
            }
        }

        return ResponseEntity.ok(
                "READ QUORUM OK (r=" + r + "). NewestFrom=" + newest.node +
                        " version=" + newest.value.getVersion() +
                        " value=" + newest.value.getValue() +
                        " HealthyCandidates=" + healthyReplicas
        );
    }

    private static class ReplicaRead {
        String node;
        VersionedValue value;

        ReplicaRead(String node, VersionedValue value) {
            this.node = node;
            this.value = value;
        }

        @Override
        public String toString() {
            return "{node=" + node + ", version=" + (value == null ? "null" : value.getVersion()) + "}";
        }
    }
}
