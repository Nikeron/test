package ru.nikeron.test;

import java.io.IOException;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatController {

    @Value("${analyze-file}")
    private String analyzeFilePath;
    @Value("${cache-dir}")
    private String cacheDir;
    @Value("${chunk-duration}")
    private long chunkDuration;
    @Value("${chunk-buffer}")
    private int chunkBuffer;

    private StatChunker statChunker;

    @PostConstruct
    void postConstruct() throws ClassNotFoundException, IOException {
        statChunker = new StatChunker(analyzeFilePath, cacheDir, chunkDuration, chunkBuffer);
    }

    @GetMapping(path = "/pages")
    public Set<Long> pageCount() {
        return statChunker.getPages();
    }

}
