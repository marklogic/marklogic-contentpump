package com.marklogic.contentpump;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

/**
 * The settings of running unit tests
 * 
 * @author ali
 *
 */
public class Constants {
    /**
     * Home of mlcp. Must set if unit tests run in distributed mode
     */
    public static String MLCP_HOME;
    static{
       try {
            MLCP_HOME = new java.io.File( "." ).getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }    
    }
    private static String TEST_DATA = "//////" + MLCP_HOME + "/src/test/resources";
    /**
     * Path of test data
     */
    public static Path TEST_PATH = new Path("file", null, TEST_DATA);
    /**
     * Path of output
     * Default: /tmp/mlcpout
     */
    public static Path OUT_PATH;
    static {
        String path = System.getProperty("OUTPUT_PATH", "/tmp/mlcpout");
        OUT_PATH = new Path("file", null, path);
    }
    /**
     * conf directory of hadoop. Must set if unit tests run in distributed mode
     */
    public static String HADOOP_CONF_DIR;
    static {
        HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
    }
    /**
     * Port of MarkLogic Server to communicate to.
     * Default: 8000
     */
    public static String port;
    static {
        port = System.getProperty("PORT", "8000");
    }
    /**
     * Destination database of COPY operations.
     * Default: CopyDst
     */
    public static String copyDst;
    static {
        copyDst = System.getProperty("COPY_DST", "CopyDst");
    }
    /**
     * Database used in tests. 
     * Default: Document
     */
    public static String testDb;
    static {
        testDb = System.getProperty("TEST_DB", "Documents");
    }
}
