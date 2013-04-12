package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestImportArchive {
    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testBug19403() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/archive"
            + " -input_file_type archive";
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
    }
    
    @Test
    public void testArchiveWithNaked() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/mixnakedzip"
            + " -input_file_type archive";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", 
            "fn:count(" +
            "for $i in cts:search(xdmp:document-properties(),cts:not-query(cts:document-fragment-query(cts:and-query( () )))) \n" +
            "where fn:contains(fn:base-uri($i),\"naked\") eq fn:false() \n" +
            "return fn:base-uri($i)" +
            ")");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
    }
    
    @Test
    public void testBug19403_1() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/archive/wiki-000001.zip"
            + " -input_file_type archive";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());
        Utils.closeSession();
    }
}
