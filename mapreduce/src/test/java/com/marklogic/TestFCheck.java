package com.marklogic;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

import com.marklogic.mapreduce.test.FCheck;

public class TestFCheck {
    boolean verbose = false;
    String testData = "src/test/resources";
    
    @Test
    public void testFCheck() throws IOException {
        new FCheck(verbose).fcheck(new File(testData, "3doc-test/3docForest"));
    }
}
