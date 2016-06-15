package com.marklogic;

public class ForestData {
    String path;
    String name;
    String stand;
    int numDocs;
    public ForestData(String path, String name, String stand, int numDocs) {
        super();
        this.path = path;
        this.name = name;
        this.stand = stand;
        this.numDocs = numDocs;
    }
    public String getPath() {
        return path;
    }
    public String getName() {
        return name;
    }
    public String getStand() {
        return stand;
    }
    public int getNumDocs() {
        return numDocs;
    }
    
    
}
