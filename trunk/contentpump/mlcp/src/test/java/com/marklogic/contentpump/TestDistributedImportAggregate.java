package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + "/sample-agg/agg3.xml"
            + " -input_file_type aggregates"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
}
