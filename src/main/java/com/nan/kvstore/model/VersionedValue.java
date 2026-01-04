package com.nan.kvstore.model;

/*
  VersionedValue is what we store for each key.
  - value: the actual string value
  - version: a number that increases when the value is updated
*/
public class VersionedValue {
    private String value;
    private long version;

    // Default constructor required by Spring/Jackson for JSON serialization
    public VersionedValue() {}

    public VersionedValue(String value, long version) {
        this.value = value;
        this.version = version;
    }

    public String getValue() { return value; }
    public long getVersion() { return version; }

    public void setValue(String value) { this.value = value; }
    public void setVersion(long version) { this.version = version; }
}
