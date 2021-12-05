package ru.nikeron.test;

import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;

import org.springframework.stereotype.Service;

@Service
public class StatService { // service to initialize cache
    private StatChunker statChunker;

    public StatService(StatProperties properties) throws ClassNotFoundException, IOException {
        statChunker = new StatChunker(properties.getAnalyzeFilePath(), properties.getCacheDir(),
                properties.getChunkDuration(), properties.getChunkBuffer()); // define chunker with property values
    }

    public Set<Long> getPages() {
        return statChunker.getPages();
    }

    public void parallelPageWalker(long pageId, long from, long to, Consumer<StatCacheChunk.StatCacheRecord> consumer)
            throws ClassNotFoundException, IOException {
        statChunker.parallelPageWalker(pageId, from, to, consumer);
    }
}
