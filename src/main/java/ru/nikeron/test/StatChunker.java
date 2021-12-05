package ru.nikeron.test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileSystemUtils;

import ru.nikeron.test.StatCacheChunk.StatCacheRecord;

public class StatChunker {
    private static final String CACHE_INFO_FILE_NAME = "_.ser";
    private static final int DEFAULT_TIME_CHUNKS_BUFFER_SIZE = 3;

    Logger logger = LoggerFactory.getLogger(StatChunker.class);

    private String cacheDirPath;
    private StatCacheInfo cacheInfo;

    public StatChunker(String csvFilePath, String cacheDirPath, long chunkDuration)
            throws ClassNotFoundException, IOException {
        this(csvFilePath, cacheDirPath, chunkDuration, DEFAULT_TIME_CHUNKS_BUFFER_SIZE);
    }

    public StatChunker(String csvFilePath, String cacheDirPath, long chunkDuration, int timeChunksBufferSize)
            throws ClassNotFoundException, IOException {
        File csvFile = new File(csvFilePath);
        // check file is a file
        if (!csvFile.exists())
            throw new FileNotFoundException(csvFilePath + " is not exists!");
        if (!csvFile.isFile())
            throw new FileNotFoundException(csvFilePath + " is not a file!");

        this.cacheDirPath = cacheDirPath;

        // check if data already cached
        boolean generateNewCache = false;
        try {
            cacheInfo = loadCacheInfo();
            generateNewCache = !cacheInfo.equals(new StatCacheInfo(csvFile, chunkDuration));
        } catch (Exception ex) {
            generateNewCache = true;
        }
        if (generateNewCache) // generate cache if need
            generateCache(csvFile, chunkDuration, timeChunksBufferSize);
    }

    @SuppressWarnings("unchecked")
    private static <T> T loadObject(File file)
            throws FileNotFoundException, IOException, ClassNotFoundException { // save a generic object to a file
        try (FileInputStream fileInputStream = new FileInputStream(file);
                BufferedInputStream bufferedFileInputStream = new BufferedInputStream(fileInputStream);
                ObjectInputStream objectInputStream = new ObjectInputStream(bufferedFileInputStream)) {
            return (T) objectInputStream.readObject();
        }
    }

    private static <T> void saveObject(File file, T obj)
            throws FileNotFoundException, IOException { // load a generic object from a file
        try (FileOutputStream fileOutputStream = new FileOutputStream(file);
                BufferedOutputStream bufferedFileOutputStream = new BufferedOutputStream(fileOutputStream);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedFileOutputStream)) {
            objectOutputStream.writeObject(obj);
        }
    }

    private StatCacheInfo loadCacheInfo()
            throws FileNotFoundException, IOException, ClassNotFoundException { // loads a cache information object from
                                                                                // a file
        return loadObject(new File(cacheDirPath, CACHE_INFO_FILE_NAME));
    }

    private void saveCacheInfo(StatCacheInfo cacheInfo)
            throws FileNotFoundException, IOException { // saves a cache information object to a file
        saveObject(new File(cacheDirPath, CACHE_INFO_FILE_NAME), cacheInfo);
    }

    private String getChunkPath(long pageId, long timeChunk) { // returns a cache chunk path
        return Paths.get(cacheDirPath, pageId + " - " + timeChunk + ".ser").toString();
    }

    private StatCacheChunk loadCacheChunk(long pageId, long timeChunk)
            throws ClassNotFoundException, FileNotFoundException, IOException { // loads a cached chunk from a file
        return loadObject(new File(getChunkPath(pageId, timeChunk)));
    }

    private void saveCacheChunk(long pageId, long timeChunk, StatCacheChunk chunk)
            throws FileNotFoundException, IOException { // saves a cached chunk to a file
        saveObject(new File(getChunkPath(pageId, timeChunk)), chunk);
    }

    private void generateCache(File csvFile, long chunkDuration, int timeChunksBufferSize)
            throws IOException { // generate a cache in a given directory
        FileSystemUtils.deleteRecursively(Path.of(cacheDirPath)); // first recreate the cache directory
        new File(cacheDirPath).mkdirs();

        cacheInfo = new StatCacheInfo(csvFile, chunkDuration); // new cache info object (will be saved in a file)
        AtomicLong lineCounter = new AtomicLong(0); // count number of parsed lines
        Set<Long> pageSet = Collections.synchronizedSet(new HashSet<>()); // set of unique page id's
        // map, where key is a page id, contains lock and map of chunks by time
        Map<Long, Entry<ReentrantReadWriteLock, Map<Long, StatCacheChunk>>> chunkMapsPerPageMap = new HashMap<>();
        // read-write lock of that map to be sure we didn't put a concurrent value
        ReentrantReadWriteLock chunkMapsPrePageRWLock = new ReentrantReadWriteLock();

        try (FileReader csvFileReader = new FileReader(csvFile); // open buffered reader of a file
                BufferedReader csvBufferedReader = new BufferedReader(csvFileReader)) {
            csvBufferedReader.lines().parallel().forEach(line -> { // read file by lines in parralel stream
                lineCounter.incrementAndGet(); // increment atomic line counter
                String[] splittedLine = line.split(","); // split the line and check if it is correct
                if (splittedLine.length < 3) {
                    logger.error(line + " line is not acceptable!");
                    return;
                }
                long UID, pageId, timestamp; // parse values from a split line
                try {
                    UID = Long.parseLong(splittedLine[0]);
                    pageId = Long.parseLong(splittedLine[1]);
                    timestamp = Long.parseLong(splittedLine[2]);
                } catch (NumberFormatException ex) {
                    logger.error("can't parse line", ex);
                    return;
                }
                long timeChunk = timestamp / chunkDuration; // time chunks are timestamps divided by chunk duration
                // record that will be saved
                StatCacheChunk.StatCacheRecord record = new StatCacheChunk.StatCacheRecord(UID, timestamp);

                Entry<ReentrantReadWriteLock, Map<Long, StatCacheChunk>> chunksPerTimeMap = getChunksPerTimeMap(
                        chunkMapsPerPageMap, chunkMapsPrePageRWLock, pageId, pageSet);// chunk map for page
                putRecordToCache(chunksPerTimeMap, pageId, timeChunk, record, timeChunksBufferSize); // cache record
            });
        }
        // save all chunks that currently stays in memory
        chunkMapsPerPageMap.entrySet().parallelStream().forEach(pageEntry -> {
            pageEntry.getValue().getValue().entrySet().parallelStream().forEach(chunkEntry -> {
                try {
                    saveCacheChunk(pageEntry.getKey(), chunkEntry.getKey(), chunkEntry.getValue());
                } catch (IOException ex) {
                    logger.error("can't save cached chunk", ex);
                }
            });
        });
        cacheInfo.setLineCount(lineCounter.get());
        cacheInfo.setPages(pageSet);
        saveCacheInfo(cacheInfo); // save information about cache for the future comparison

        logger.info(lineCounter.get() + " lines parsed. " + pageSet.size() + " unique pages.");
    }

    private Entry<ReentrantReadWriteLock, Map<Long, StatCacheChunk>> getChunksPerTimeMap(
            Map<Long, Entry<ReentrantReadWriteLock, Map<Long, StatCacheChunk>>> chunkMapsPerPageMap,
            ReentrantReadWriteLock chunkMapsPrePageRWLock, long pageId, Set<Long> pageSet) {
        Entry<ReentrantReadWriteLock, Map<Long, StatCacheChunk>> chunksPerTimeMap; // return value
        chunkMapsPrePageRWLock.readLock().lock(); // pages map read lock
        try {
            chunksPerTimeMap = chunkMapsPerPageMap.get(pageId); // get chunk map for page
            if (chunksPerTimeMap == null) { // if page not exists
                chunkMapsPrePageRWLock.readLock().unlock(); // relock to write lock
                chunkMapsPrePageRWLock.writeLock().lock();
                try {
                    chunksPerTimeMap = chunkMapsPerPageMap.get(pageId); // recheck for concurrent creation
                    if (chunksPerTimeMap == null) { // if still not exists -> create one
                        chunksPerTimeMap = Map.entry(new ReentrantReadWriteLock(), new HashMap<>());
                        pageSet.add(pageId);
                        chunkMapsPerPageMap.put(pageId, chunksPerTimeMap);
                    }
                    chunkMapsPrePageRWLock.readLock().lock(); // downgrade lock level to read lock
                } finally {
                    chunkMapsPrePageRWLock.writeLock().unlock();
                }
            }
        } finally {
            chunkMapsPrePageRWLock.readLock().unlock(); // unlock pages map
        }
        return chunksPerTimeMap;
    }

    private void putRecordToCache(Entry<ReentrantReadWriteLock, Map<Long, StatCacheChunk>> chunksPerTimeMap,
            long pageId, long timeChunk, StatCacheRecord record, int timeChunksBufferSize) {
        StatCacheChunk cacheChunk; // cache chunk of page by time chunk
        chunksPerTimeMap.getKey().readLock().lock(); // lock page
        try {
            cacheChunk = chunksPerTimeMap.getValue().get(timeChunk); // get cache chunk by time chunk
            if (cacheChunk == null) { // if not exists
                chunksPerTimeMap.getKey().readLock().unlock(); // upgrade to write lock
                chunksPerTimeMap.getKey().writeLock().lock();
                try {
                    try {
                        cacheChunk = chunksPerTimeMap.getValue().get(timeChunk); // recheck for concurrent creation
                        if (cacheChunk == null) { // if not exists
                            try { // load or create one
                                cacheChunk = loadCacheChunk(pageId, timeChunk);
                            } catch (FileNotFoundException ex) {
                                cacheChunk = new StatCacheChunk();
                            } catch (Exception ex) {
                                logger.error("can't load cached chunk", ex);
                                return;
                            }
                            // release extra chunks
                            while (chunksPerTimeMap.getValue().size() >= timeChunksBufferSize) {
                                // get minimum from time chunks
                                Long toUnloadTimeChunk = Collections.min(chunksPerTimeMap.getValue().keySet());
                                try { // save and remove from the chunks map
                                    saveCacheChunk(pageId, toUnloadTimeChunk,
                                            chunksPerTimeMap.getValue().get(toUnloadTimeChunk));
                                    chunksPerTimeMap.getValue().remove(toUnloadTimeChunk);
                                } catch (IOException ex) {
                                    logger.error("can't save cached chunk", ex);
                                    return;
                                }
                            }
                            // put to the chunk map of a page
                            chunksPerTimeMap.getValue().put(timeChunk, cacheChunk);
                        }
                    } finally {
                        chunksPerTimeMap.getKey().readLock().lock(); // downgrade lock level to read lock
                    }
                } finally {
                    chunksPerTimeMap.getKey().writeLock().unlock(); // unlock write lock of the page chunks
                }
            }
            cacheChunk.addRecord(record); // and add record to the chunk
        } finally {
            chunksPerTimeMap.getKey().readLock().unlock(); // unlock read lock of the page chunks
        }
    }

    public Set<Long> getPages() {
        return cacheInfo.getPages();
    }
}
