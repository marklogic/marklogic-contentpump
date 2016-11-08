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

public class TestDistributedImportDelimitedText {
    @Before
    public void setup() {
        assertNotNull("No HADOOP_CONF_DIR found!", Constants.HADOOP_CONF_DIR);
    }

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportDelimitedText() throws Exception {
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
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
        assertEquals("7", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedTextDocJSONWithOptions() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path "
                + Constants.TEST_PATH.toUri()
                + "/csv"
                + " -hadoop_conf_dir "
                + Constants.HADOOP_CONF_DIR
                + " -input_file_type delimited_text -input_file_pattern sample.+\\.csv"
                + " -document_type json"
                + " -split_input true -delimited_uri_id first"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(Utils.getTestDbXccUri(),
                        "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
        Utils.closeSession();
        
        result = Utils.assertDocsFormat(Utils.getTestDbXccUri(),"JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        Utils.closeSession();
        
        result = Utils.getAllDocs(Utils.getTestDbXccUri());
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }

        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedText#testImportDelimitedTextDocJSONWithOptions.txt");
        assertTrue(sb.toString().equals(key));
    }
    
}
