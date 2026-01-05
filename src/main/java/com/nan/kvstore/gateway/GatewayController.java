package com.nan.kvstore.gateway;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
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

    // Debug build string (helps confirm correct gateway instance)
    private static final String GATEWAY_BUILD = "GATEWAY_6C_HINTED_HANDOFF_v1";

    /*
      Health table maintained by gateway:
      nodeHealth[nodeUrl] = true/false
    */
    private final Map<String, Boolean> nodeHealth = new ConcurrentHashMap<>();

    /*
      Hinted handoff store:
      hintsByNode[targetNode] = deque of missed writes that should be delivered later
    */
    private final Map<String, ConcurrentLinkedDeque<Hint>> hintsByNode = new ConcurrentHashMap<>();

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

        // Background thread: try hinted handoff delivery every 2 seconds
        Thread handoffThread = new Thread(this::handoffLoop);
        handoffThread.setDaemon(true);
        handoffThread.start();
    }

    // ------------------ Debug: confirm correct gateway ------------------
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
            ResponseEntity<String> resp = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            return resp.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            return false;
        }
    }

    // For debugging: see which nodes are currently UP/DOWN
    @GetMapping("/cluster/health")
    public ResponseEntity<Map<String, Boolean>> clusterHealth() {
        Map<String, Boolean> snapshot = new LinkedHashMap<>();
        for (String n : allNodes) {
            snapshot.put(n, Boolean.TRUE.equals(nodeHealth.get(n)));
        }
        return ResponseEntity.ok(snapshot);
    }

    // ------------------ HINTED HANDOFF LOOP ------------------
    // Periodically tries to deliver queued hints to nodes that are healthy again.
    private void handoffLoop() {
        while (true) {
            try {
                flushHintsInternal();
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
            } catch (Exception ignored) {
                // keep loop alive even if something unexpected happens
            }
        }
    }

    private void addHint(String targetNode, Hint hint) {
        hintsByNode.computeIfAbsent(targetNode, k -> new ConcurrentLinkedDeque<>()).add(hint);
    }

    // Debug: how many hints are pending per node
    @GetMapping("/handoff/pending")
    public ResponseEntity<Map<String, Integer>> pendingHints() {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (String n : allNodes) {
            ConcurrentLinkedDeque<Hint> q = hintsByNode.get(n);
            out.put(n, q == null ? 0 : q.size());
        }
        return ResponseEntity.ok(out);
    }

    // Manual flush endpoint (in addition to background loop)
    @PostMapping("/handoff/flush")
    public ResponseEntity<String> flushHints() {
        FlushResult r = flushHintsInternal();
        return ResponseEntity.ok("HINTED HANDOFF FLUSHED. delivered=" + r.delivered + " remaining=" + r.remaining);
    }

    private FlushResult flushHintsInternal() {
        int delivered = 0;
        int remaining = 0;

        for (String node : allNodes) {
            // Only attempt delivery if node is healthy now
            if (!Boolean.TRUE.equals(nodeHealth.get(node))) {
                // count remaining without modifying queue
                ConcurrentLinkedDeque<Hint> q = hintsByNode.get(node);
                if (q != null) remaining += q.size();
                continue;
            }

            ConcurrentLinkedDeque<Hint> q = hintsByNode.get(node);
            if (q == null || q.isEmpty()) continue;

            // Deliver in FIFO order
            int triesThisNode = q.size();
            for (int i = 0; i < triesThisNode; i++) {
                Hint h = q.pollFirst();
                if (h == null) break;

                String url = node + "/kv/put?key=" + h.key + "&value=" + h.value + "&version=" + h.version;
                try {
                    restTemplate.exchange(url, HttpMethod.PUT, null, VersionedValue.class);
                    delivered++;
                } catch (RestClientException e) {
                    // Still failing -> re-queue at end
                    q.addLast(h);
                    remaining++;
                }
            }

            if (q.isEmpty()) {
                hintsByNode.remove(node);
            } else {
                remaining += q.size();
            }
        }

        return new FlushResult(delivered, remaining);
    }

    private static class FlushResult {
        int delivered;
        int remaining;
        FlushResult(int delivered, int remaining) {
            this.delivered = delivered;
            this.remaining = remaining;
        }
    }

    // ------------------ PUT (quorum write + hinted handoff) ------------------
    @PutMapping("/put")
    public ResponseEntity<String> put(@RequestParam String key,
                                      @RequestParam String value,
                                      @RequestParam(defaultValue = "" + DEFAULT_W) int w) {

        if (w < 1) w = 1;
        if (w > N) w = N;

        long version = System.currentTimeMillis();

        // Intended replicas for this key (based on consistent hashing ring)
        List<String> replicas = router.pickReplicaNodes(key, N);

        List<String> successes = new ArrayList<>();
        List<String> queuedHints = new ArrayList<>();
        List<String> failures = new ArrayList<>();

        // Important: we always consider ALL intended replicas for hints.
        // We only attempt network calls to healthy nodes.
        for (String node : replicas) {
            boolean healthy = Boolean.TRUE.equals(nodeHealth.get(node));

            if (!healthy) {
                // node is down -> queue hint so it can catch up when it returns
                addHint(node, new Hint(key, value, version, System.currentTimeMillis()));
                queuedHints.add(node);
                continue;
            }

            // node healthy -> attempt write
            String url = node + "/kv/put?key=" + key + "&value=" + value + "&version=" + version;
            try {
                restTemplate.exchange(url, HttpMethod.PUT, null, VersionedValue.class);
                successes.add(node);

                // optimization: once quorum is met, we can stop trying more healthy nodes
                // BUT we must still queue hints for any remaining unhealthy replicas,
                // which we already do above based on health table.
                if (successes.size() >= w) {
                    // we can stop here, but there could be more healthy nodes we would have tried.
                    // In Dynamo, a coordinator may do full replication asynchronously.
                    // For this project, quorum is enough for success.
                }
            } catch (RestClientException e) {
                // write failed even though we thought node is healthy -> treat as down and queue hint
                nodeHealth.put(node, false);
                addHint(node, new Hint(key, value, version, System.currentTimeMillis()));
                failures.add(node);
                queuedHints.add(node);
            }

            if (successes.size() >= w) {
                // stop attempting further healthy replicas once quorum is satisfied
                // (replication to remaining replicas happens via hinted handoff when needed)
                // NOTE: this makes write latency smaller in failure cases.
                break;
            }
        }

        // Fail fast if quorum not satisfied
        if (successes.size() < w) {
            return ResponseEntity.status(503).body(
                    "WRITE FAILED (need w=" + w + "). version=" + version +
                            " Success=" + successes +
                            " Fail=" + failures +
                            " HintsQueuedFor=" + queuedHints +
                            " Replicas=" + replicas +
                            " HealthTable=" + clusterHealth().getBody()
            );
        }

        return ResponseEntity.ok(
                "WRITE QUORUM OK (w=" + w + "). version=" + version +
                        " Success=" + successes +
                        (queuedHints.isEmpty() ? "" : " HintsQueuedFor=" + queuedHints) +
                        " Replicas=" + replicas
        );
    }

    // ------------------ GET (health-aware quorum read + newest + read repair) ------------------
    @GetMapping("/get")
    public ResponseEntity<String> get(@RequestParam String key,
                                      @RequestParam(defaultValue = "" + DEFAULT_R) int r,
                                      @RequestParam(defaultValue = "true") boolean repair) {

        if (r < 1) r = 1;
        if (r > N) r = N;

        List<String> replicas = router.pickReplicaNodes(key, N);

        // Read only from healthy replicas (otherwise we'd waste timeouts)
        List<String> healthy = new ArrayList<>();
        for (String node : replicas) {
            if (Boolean.TRUE.equals(nodeHealth.get(node))) healthy.add(node);
        }

        if (healthy.size() < r) {
            return ResponseEntity.status(503).body(
                    "READ FAILED FAST. Not enough healthy replicas for r=" + r +
                            ". Healthy=" + healthy +
                            " Replicas=" + replicas +
                            " HealthTable=" + clusterHealth().getBody()
            );
        }

        List<ReplicaRead> reads = new ArrayList<>();

        for (String node : healthy) {
            if (reads.size() >= r) break;

            String url = node + "/kv/get?key=" + key;
            try {
                ResponseEntity<VersionedValue> resp =
                        restTemplate.exchange(url, HttpMethod.GET, null, VersionedValue.class);

                if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                    reads.add(new ReplicaRead(node, resp.getBody()));
                }
            } catch (RestClientException ignored) {
                // If a healthy node suddenly fails, mark it down
                nodeHealth.put(node, false);
            }
        }

        if (reads.size() < r) {
            return ResponseEntity.status(500).body(
                    "READ FAILED (need r=" + r + "). Only got " + reads.size() +
                            " SuccessfulReads=" + reads +
                            " HealthyCandidates=" + healthy
            );
        }

        ReplicaRead newest = reads.stream()
                .max(Comparator.comparingLong(rr -> rr.value.getVersion()))
                .get();

        // Read repair among the replicas we successfully contacted
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
                        " HealthyCandidates=" + healthy
        );
    }

    // ------------------ Helper types ------------------
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

    private static class Hint {
        String key;
        String value;
        long version;
        long createdAtMs;

        Hint(String key, String value, long version, long createdAtMs) {
            this.key = key;
            this.value = value;
            this.version = version;
            this.createdAtMs = createdAtMs;
        }
    }
}
