package ru.nikeron.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatCacheChunk implements Serializable {
    public static class StatCacheRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        public long UID, timestamp;

        public StatCacheRecord(long UID, long timestamp) {
            this.UID = UID;
            this.timestamp = timestamp;
        }
    }

    private static final long serialVersionUID = 1L;

    List<StatCacheRecord> records; // = new ArrayList<>();

    public StatCacheChunk() {
        records = Collections.synchronizedList(new ArrayList<>());
    }

    public void addRecord(StatCacheRecord record) {
        records.add(record);
    }
}
