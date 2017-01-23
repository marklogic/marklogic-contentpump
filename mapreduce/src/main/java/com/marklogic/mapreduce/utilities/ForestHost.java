package com.marklogic.mapreduce.utilities;

public class ForestHost {
    private String forest;
    private String hostName;

    public ForestHost(String forest, String hostName) {
        this.forest = forest;
        this.hostName = hostName;
    }

    public String getForest() {
        return forest;
    }

    public String getHostName() {
        return hostName;
    }
}

