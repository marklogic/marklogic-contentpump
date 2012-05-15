package com.marklogic.contentpump;

import java.util.concurrent.atomic.AtomicInteger;

public class LocalIdGenerator implements IdGenerator {
    private AtomicInteger autoid;
    private String base;
    
    public LocalIdGenerator(String base) {
        autoid = new AtomicInteger(0);
        this.base = base != null ? base + "-" : "";
    }

    @Override
    public String incrementAndGet() {
        return base + autoid.incrementAndGet();
    }

}
