# About precision

The Jaccard index formula with default double precision is used for the calculation.

# How to run

Use gradle to build project:

```
gradlew bootJar
```

Execute program with 4G+ heap size:

```
java -Xmx4G -jar build/libs/test-0.0.1-SNAPSHOT.jar --stat.analyzeFilePath=uid_page_timestamp.sorted.csv --stat.cacheDir=cache --stat.chunkDuration=3600 --stat.chunkBuffer=3
```

## Options:
```
--stat.analyzeFilePath - path to csv file
--stat.cacheDir - path to cache directory
--stat.chunkDuration - cache chunk duration in seconds
--stat.chunkBuffer - chunks per page in memory
```
