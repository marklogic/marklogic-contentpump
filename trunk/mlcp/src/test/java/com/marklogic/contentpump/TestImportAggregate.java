package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestImportAggregate {

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedline() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -mode local -thread_count 1"// -aggregate_uri_id PMID"
            + " -input_file_type aggregates"
            + " -output_uri_replace " + Constants.TEST_PATH.toUri().getPath() + ",'/medline'";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportEscapeQuoteInAttr() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/escapequote.xml"
            + " -mode local -thread_count 1 -aggregate_record_element parent"
            + " -input_file_type aggregates"
            + " -output_uri_prefix /data/ -output_uri_suffix .xml";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
//    @Test
    public void testbug19151() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path" //+ Constants.TEST_PATH.toUri()
//            + "/agg/medline04.small.xml"
            + " /space2/qa/mlcp/data/agg/bug19151"
            + " -mode local -thread_count 4"// -transaction_size 1 -batch_size 100"
            + " -aggregate_uri_id ArticleTitle"
            + " -input_file_type aggregates"
            + " -thread_count 1" //comment this line to reproduce
            + " -output_uri_replace \"\\[,'',\\],'',:,''\"";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1162", result.next().asString());
        Utils.closeSession();
    }
    
    
    /*
     * multithread mapper to load a file with 4 threads (thread_count is 4 by
     * default)
     */
    @Test
    public void testImportMedlineMultiWriter() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -mode local -aggregate_uri_id PMID"
            + " -input_file_type aggregates";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineISO88591() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/encoding/medline04.small.iso-8859-1.xml -content_encoding iso-8859-1"
            + " -mode local -thread_count 1 -aggregate_uri_id PMID"
            + " -input_file_type aggregates";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineISO88591Zip() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/encoding/medline04.small.iso-8859-1.zip -content_encoding iso-8859-1"
            + " -mode local -thread_count 1 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineZip() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04.zip"
//            + "/home/ali/tmp/medlinezip"
            + " -mode local -thread_count 2 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed -input_compressed true";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformMedlineZip() throws Exception {
        Utils.prepareModule("xcc://admin:admin@localhost:5275", "/lc.xqy");
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04.zip"
//          +  "/home/ali/tmp/medlinezip"
            + " -transform_module /lc.xqy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -mode local -thread_count 2 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed -input_compressed true";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformMedlineZipFast() throws Exception {
        Utils.prepareModule("xcc://admin:admin@localhost:5275", "/lc.xqy");
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04.zip"
//            + "/home/ali/tmp/medlinezip"
            + " -fastload"
            + " -transform_module /lc.xqy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -mode local -thread_count 2 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed -input_compressed true";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testBug19146() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -mode local -aggregate_record_element ArticleTitle"
            + " -input_file_type aggregates";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
//    @Test
    public void testBadXML() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/bad.xml"
            + " -mode local -aggregate_record_element r"
            + " -input_file_type aggregates";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportAggZip() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -mode local" +
        		" -username admin" + " -password admin" + 
        		" -input_file_path " + Constants.TEST_PATH.toUri() + "/agg.zip" + 
        		" -aggregate_record_element p -aggregate_uri_id id" +
        		" -input_file_type aggregates -input_compressed true" +
        		" -input_compression_codec zip" +
        		" -namespace http://marklogic.com/foo";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath() + "/keys/TestImportAggregate#testImportAggZip.txt");
        assertTrue(sb.toString().equals(key));
    }
    

}
