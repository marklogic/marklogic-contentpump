package com.marklogic.mapreduce;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import com.marklogic.mapreduce.test.FCheck;

public class TestFCheck extends TestCase {
    boolean verbose = false;
    String testData = "src/test/resources";

    public void testFCheck() throws IOException {
        new FCheck(verbose).fcheck(new File(testData, "3docForest"));
    }
}
