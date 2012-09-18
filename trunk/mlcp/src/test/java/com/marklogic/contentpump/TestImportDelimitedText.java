package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestImportDelimitedText{

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportDelimitedText() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
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
    public void testImportDelimitedTextUTF16LE() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() 
            + "/encoding/samplecsv.utf16le.csv -content_encoding utf-16le"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextUTF16LE.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedTextUTF16BE() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() 
            + "/encoding/samplecsv.utf16be.csv -content_encoding utf-16be"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextUTF16BE.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedTextUTF16LEZip() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() 
            + "/encoding/samplecsv.utf16le.zip -content_encoding utf-16le"
            + " -delimited_uri_id first -input_compressed"
            + " -input_file_type delimited_text";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextUTF16LEZip.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedTextZip() throws Exception {
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextZip.txt");
        assertTrue(sb.toString().equals(key));
    }
}
