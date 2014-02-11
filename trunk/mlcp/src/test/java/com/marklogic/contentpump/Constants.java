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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    private static String TEST_DATA = "//////" + MLCP_HOME + "/src/test/resources";
    /**
     * Path of test data
     */
    public static Path TEST_PATH = new Path("file", null, TEST_DATA);
    
    public static Properties prop = new Properties(); 
    static {
        //load a properties file from class path, inside static method
        try {
            prop.load(new FileInputStream(MLCP_HOME + "/src/conf/test.properties"));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static Path OUT_PATH;
    static {
        String path = prop.getProperty("OUT_PATH", "/tmp/mlcpout");
        OUT_PATH = new Path("file", null, path);
    }
    
   
    /**
     * mlcp version. Must set if unit tests run in distributed mode
     */
    public static String CONTENTPUMP_VERSION = prop.getProperty("CONTENTPUMP_VERSION");
    
    /**
     * lib directory of mlcp binary package extracted. Must set if unit tests run in distributed mode
     */
    public static String CONTENTPUMP_HOME;
    static {
        String version = prop.getProperty("HADOOP", "1");
        CONTENTPUMP_HOME = MLCP_HOME + "/target/mlcp-Hadoop" + version + "-"+ CONTENTPUMP_VERSION + "-lib";
    }
    /**
     * conf directory of hadoop. Must set if unit tests run in distributed mode
     */
    public static String HADOOP_CONF_DIR;
    static {
        HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
        if (HADOOP_CONF_DIR == null) {
            HADOOP_CONF_DIR = prop.getProperty("HADOOP_CONF_DIR");
        }
    }
//    public static String HADOOP_CONF_DIR = null;
}
