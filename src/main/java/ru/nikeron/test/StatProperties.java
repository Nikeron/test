package ru.nikeron.test;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "stat")
public class StatProperties {
    private String analyzeFilePath; // path of file to analyze
    private String cacheDir; // path of cache directory
    private long chunkDuration; // chunk duration
    private int chunkBuffer; // number of chunks in memory per page

    public String getAnalyzeFilePath() {
        return analyzeFilePath;
    }

    public void setAnalyzeFilePath(String analyzeFilePath) {
        this.analyzeFilePath = analyzeFilePath;
    }

    public int getChunkBuffer() {
        return chunkBuffer;
    }

    public void setChunkBuffer(int chunkBuffer) {
        this.chunkBuffer = chunkBuffer;
    }

    public long getChunkDuration() {
        return chunkDuration;
    }

    public void setChunkDuration(long chunkDuration) {
        this.chunkDuration = chunkDuration;
    }

    public String getCacheDir() {
        return cacheDir;
    }

    public void setCacheDir(String cacheDir) {
        this.cacheDir = cacheDir;
    }
}
