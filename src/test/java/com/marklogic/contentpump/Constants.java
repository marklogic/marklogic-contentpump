package com.marklogic.contentpump;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

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
     * Default: 5276
     */
    public static String port;
    static {
        port = System.getProperty("TEST_DB_PORT", "5276");
    }
    /**
     * App server port of Copy database
     * Default: 6276
     */
    public static String copyDbPort;
    static {
        copyDbPort = System.getProperty("COPY_DB_PORT", "6276");
    }
    /**
     * Destination database of COPY operations.
     * Default: CopyDst
     */
    public static String copyDst;
    static {
        copyDst = System.getProperty("COPY_DB", "mlcp-unittest-copy-db");
    }
    /**
     * Database used in tests. 
     * Default: Document
     */
    public static String testDb;
    static {
        testDb = System.getProperty("TEST_DB", "mlcp-unittest-db");
    }
    /**
     * Static setup for all tests
     */
    static {
        File createIdxScript = new File("//////" + MLCP_HOME + "/src/test/bootstrap/createIndex.sjs");
        String createIdxQry = "";
        try {
            createIdxQry = FileUtils.readFileToString(createIdxScript);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        StringBuilder buf = new StringBuilder();
        buf.append("var testDbName = \"");
        buf.append(Constants.testDb);
        buf.append("\"\n");
        buf.append("var copyDbName = \"");
        buf.append(Constants.copyDst);
        buf.append("\"\n");
        buf.append(createIdxQry);
        try {
            Utils.runQuery(Utils.getInitDbXccUri(), buf.toString(), "javascript");
        } catch (XccConfigException | RequestException | URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        File createAxisScript = new File("//////" + MLCP_HOME + "/src/test/bootstrap/createAxis.sjs");
        String createAxisQry = "";
        try {
            createAxisQry = FileUtils.readFileToString(createAxisScript);
        } catch (IOException e) {
         // TODO Auto-generated catch block
            e.printStackTrace();
        }
        buf = new StringBuilder();
        buf.append(createAxisQry);
        try {
            Utils.runQuery(Utils.getTestDbXccUri(), buf.toString(), "javascript");
        } catch (XccConfigException | RequestException | URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
