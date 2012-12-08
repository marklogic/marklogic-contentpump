package com.marklogic.contentpump;

import org.apache.hadoop.fs.Path;

/**
 * The settings of running unit tests
 * 
 * @author ali
 *
 */
public class Constants {
    private static String WORKSPACE = "/space/projects/";
    /**
     * Home of mlcp. Must set if unit tests run in distributed mode
     */
    public static String MLCP_HOME = WORKSPACE + "/xcc/mlcp";
    
    private static String TEST_DATA = "/home/jchen/tmp/test/resources";
    /**
     * Path of test data
     */
    public static Path TEST_PATH = new Path("file", null, TEST_DATA);
    
    public static Path OUT_PATH = new Path("file", null, "/tmp/mlcpout");
    
    /**
     * lib directory of mlcp binary package extracted. Must set if unit tests run in distributed mode
     */
    public static String CONTENTPUMP_HOME = WORKSPACE + "/xcc/mlcp/target/marklogic-contentpump-1.1/lib";
    /**
     * mlcp version. Must set if unit tests run in distributed mode
     */
    public static String CONTENTPUMP_VERSION = "1.1";
    /**
     * conf directory of hadoop. Must set if unit tests run in distributed mode
     */
    public static String HADOOP_CONF_DIR = "/space/hadoop-0.20.2/conf";
}
