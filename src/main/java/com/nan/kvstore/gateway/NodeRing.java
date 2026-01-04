package com.nan.kvstore.gateway;
import java.util.List;

/*
  NodeRing holds the list of node base URLs (our 3 nodes).
  Later we can replace this with dynamic membership.
*/
public class NodeRing {
  private final List<String> nodeBaseUrls;

    public NodeRing(List<String> nodeBaseUrls) {
        this.nodeBaseUrls = nodeBaseUrls;
    }

    public List<String> getNodeBaseUrls() {
        return nodeBaseUrls;
    }
}
