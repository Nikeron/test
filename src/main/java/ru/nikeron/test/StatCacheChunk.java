package ru.nikeron.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatCacheChunk implements Serializable { // serializable container
    public static class StatCacheRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        public long UID, timestamp;

        public StatCacheRecord(long UID, long timestamp) {
            this.UID = UID;
            this.timestamp = timestamp;
        }
    }

    private static final long serialVersionUID = 1L;

    private List<StatCacheRecord> records;

    public StatCacheChunk() {
        records = Collections.synchronizedList(new ArrayList<>());
    }

    public void addRecord(StatCacheRecord record) {
        records.add(record);
    }

    public List<StatCacheRecord> getRecords() {
        return records;
    }
}
