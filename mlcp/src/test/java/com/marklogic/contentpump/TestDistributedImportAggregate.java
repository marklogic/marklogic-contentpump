package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;
public class TestDistributedImportAggregate {
    @Before
    public void setup() {
        assertNotNull("No HADOOP_CONF_DIR found!", Constants.HADOOP_CONF_DIR);
    }
    
    @After
    public void tearDown() {
        Utils.closeSession();
    }

    /*
     * Load data from HDFS
     * Do no enable it unless /sample-agg/agg3.xml exists on HDFS 
     */
    @Test
    public void testImportDFSAutoID() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/agg/agg3.xml"
                + " -input_file_type documents"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
                + " -output_uri_replace " + Path.getPathWithoutSchemeAndAuthority(Constants.TEST_PATH) + "/agg,''"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        cmd = "EXPORT -host localhost -username admin -password admin"
                + " -output_file_path /sample-agg"
                + " -output_type document"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
                + " -port " + Constants.port + " -database " + Constants.testDb;
        
        args = cmd.split(" ");
        assertFalse(args.length == 0);
        
        expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + "/sample-agg/agg3.xml"
            + " -input_file_type aggregates"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
            + " -port " + Constants.port + " -database " + Constants.testDb;
        args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString()); 
        Utils.closeSession();
    }
}
