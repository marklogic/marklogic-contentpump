package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Test;
import org.junit.Before;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.ResultSequence;

public class TestDistributedImportDocs {
    @Before
    public void setup() {
        assertNotNull("No HADOOP_CONF_DIR found!", Constants.HADOOP_CONF_DIR);
    }
    
    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportMixedDocs() throws Exception {        
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -output_uri_replace wiki,'wiki1'"
            + " -output_uri_prefix test/"
            + " -output_collections test,ML"
            + " -fastload true"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(),
            "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        
        assertEquals("93", result.next().asString());
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }
    
    @Test
    public void testImportText() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AbacuS.xml"
            + " -output_uri_prefix ABC"
            + " -output_collections test,ML -document_type text"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
 
}
