package ru.nikeron.test;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class StatCacheInfo implements Serializable { // information about all cache
    private static final long serialVersionUID = 1L;

    private long length, lastModified; // file information
    private long lineCount, chunkDuration; // cache information
    private Set<Long> pageSet = new HashSet<>(); // cached pages

    public StatCacheInfo(File file, long chunkDuration) {
        this.length = file.length();
        this.lastModified = file.lastModified();
        this.chunkDuration = chunkDuration;
    }

    public long getChunkDuration() {
        return chunkDuration;
    }

    public long getLineCount() {
        return lineCount;
    }

    public void setLineCount(long lineCount) {
        this.lineCount = lineCount;
    }

    public Set<Long> getPages() {
        return Set.copyOf(pageSet);
    }

    public void setPages(Set<Long> pageSet) {
        this.pageSet = pageSet;
    }

    @Override
    public boolean equals(Object obj) { // equival only if length, modify date and chunk duration is equivals
        if (obj instanceof StatCacheInfo) {
            StatCacheInfo cmpObj = (StatCacheInfo) obj;
            return this.length == cmpObj.length && this.lastModified == cmpObj.lastModified &&
                    this.chunkDuration == cmpObj.chunkDuration;
        }
        return super.equals(obj);
    }
}
