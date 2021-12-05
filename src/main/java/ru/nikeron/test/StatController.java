package ru.nikeron.test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatController {

    @Autowired
    private StatService service;

    Logger logger = LoggerFactory.getLogger(StatChunker.class);

    @GetMapping(path = "/pages")
    public Set<Long> pageCount() { // return set of unique pages
        return service.getPages();
    }

    @GetMapping(path = "/similarity")
    public double similarity(@RequestParam long page1, @RequestParam long page2, @RequestParam long from,
            @RequestParam long to) throws ClassNotFoundException, IOException { // return jaccard index
        Set<Long> page1set = Collections.synchronizedSet(new HashSet<>());
        service.parallelPageWalker(page1, from, to, record -> { // set of unique uid's for page1
            if (record.timestamp >= from && record.timestamp < to)
                page1set.add(record.UID);
        });
        Set<Long> page2set = Collections.synchronizedSet(new HashSet<>());
        service.parallelPageWalker(page2, from, to, record -> {// set of unique uid's for page1
            if (record.timestamp >= from && record.timestamp < to)
                page2set.add(record.UID);
        });
        Set<Long> retainedSet = new HashSet<>(page1set); // common members of two sets
        retainedSet.retainAll(page2set);
        // https://en.wikipedia.org/wiki/Jaccard_index
        return (double) retainedSet.size() / (page1set.size() + page2set.size() - retainedSet.size());
    }
}
