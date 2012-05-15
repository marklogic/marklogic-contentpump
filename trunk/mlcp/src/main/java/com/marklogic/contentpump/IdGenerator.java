package com.marklogic.contentpump;

public interface IdGenerator {
    /**
     * Increment the id and return the id as String
     * @return id
     */
    public String incrementAndGet();
}
