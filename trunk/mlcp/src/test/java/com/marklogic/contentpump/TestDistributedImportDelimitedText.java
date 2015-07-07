package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;

public class TestDistributedImportDelimitedText {


    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportDelimitedText() throws Exception {
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");

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
    
    //4 files into 6 delimited splits
    @Test
    public void testImportDelimitedTextSplit() throws Exception {
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
            + " -delimited_uri_id first"
            + " -split_input -max_split_size 50"
            + " -input_file_type delimited_text"
            + " -input_file_pattern .*\\.csv";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");

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
    public void testImportDelimitedTextElemNamesSplit() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample3.csv.ename"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR
            + " -split_input -max_split_size 50"
            + " -input_file_type delimited_text";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextElemNamesSplit.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedTextDocJSON() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path "
                + Constants.TEST_PATH.toUri()
                + "/csv"
                + " -hadoop_conf_dir "
                + Constants.HADOOP_CONF_DIR
                + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
                + " -document_type json";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                        "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
    }
    
    @Test
    public void testImportDelimitedTextDocJSONWithOptions() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path "
                + Constants.TEST_PATH.toUri()
                + "/csv"
                + " -hadoop_conf_dir "
                + Constants.HADOOP_CONF_DIR
                + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
                + " -document_type json -delimited_root_name doc"
                + " -split_input false -delimited_uri_id first";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                        "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }

        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedText#testImportDelimitedTextDocJSONWithOptions.txt");
        //System.out.println(sb);
        assertTrue(sb.toString().equals(key));
    }
    
}
