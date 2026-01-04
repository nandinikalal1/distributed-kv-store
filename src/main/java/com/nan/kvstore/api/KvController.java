package com.nan.kvstore.api;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.nan.kvstore.model.VersionedValue;
import com.nan.kvstore.service.KvService;

@RestController
@RequestMapping("/kv")
public class KvController {

    private final KvService service;

    public KvController(KvService service) {
        this.service = service;
    }

    // Health endpoint used by gateway to check node liveness
    // GET /kv/health -> {"status":"UP"}
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    // PUT /kv/put?key=...&value=...&version=123 (version optional)
    @PutMapping("/put")
    public ResponseEntity<VersionedValue> put(@RequestParam String key,
                                              @RequestParam String value,
                                              @RequestParam(required = false) Long version) {
        VersionedValue vv = service.put(key, value, version);
        return ResponseEntity.ok(vv);
    }

    // GET /kv/get?key=...
    @GetMapping("/get")
    public ResponseEntity<VersionedValue> get(@RequestParam String key) {
        VersionedValue vv = service.get(key);
        if (vv == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(vv);
    }

    // DELETE /kv/delete?key=...
    @DeleteMapping("/delete")
    public ResponseEntity<String> delete(@RequestParam String key) {
        boolean existed = service.delete(key);
        if (!existed) return ResponseEntity.status(404).body("Key not found");
        return ResponseEntity.ok("Deleted");
    }
}
