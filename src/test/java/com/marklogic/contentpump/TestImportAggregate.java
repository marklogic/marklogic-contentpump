package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.ResultSequence;

public class TestImportAggregate {

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedline() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -thread_count 1"// -aggregate_uri_id PMID"
            + " -input_file_type aggregates"
            + " -output_uri_replace " + Constants.TEST_PATH.toUri().getPath() + ",'/medline'"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformMedlineFilenameAsCollection() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -thread_count 1"// -aggregate_uri_id PMID"
            + " -input_file_type aggregates"
            + " -transform_module /lc_test.xqy"
            + " -filename_as_collection true"
            + " -output_collections abc,cde"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -output_uri_replace " + Constants.TEST_PATH.toUri().getPath() + ",'/medline'"
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
        assertEquals("2", result.next().asString());
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(cts:collections())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportIDNameWithNS() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/lei.xml"
            + " -thread_count 1 -aggregate_uri_id LEI"
            + " -aggregate_record_namespace www.leiutility.org"
            + " -aggregate_record_element LegalEntity"
            + " -input_file_type aggregates"
            + " -output_uri_replace " + Constants.TEST_PATH.toUri().getPath() + ",'/lei'"
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
        assertEquals("4", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineUTF16LE() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.utf16.xml"
            + " -thread_count 1"// -aggregate_uri_id PMID"
            + " -input_file_type aggregates"
            + " -content_encoding UTF-16LE"
            + " -output_uri_replace " + Constants.TEST_PATH.toUri().getPath() + ",'/medline'"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportEscapeQuoteInAttr() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/escapequote.xml"
            + " -thread_count 1 -aggregate_record_element parent"
            + " -input_file_type aggregates"
            + " -output_uri_prefix /data/ -output_uri_suffix .xml"
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
        assertEquals("3", result.next().asString());
        Utils.closeSession();
    }
    
//    @Test
    public void testbug19151() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path" //+ Constants.TEST_PATH.toUri()
//            + "/agg/medline04.small.xml"
            + " /space2/qa/mlcp/data/agg/bug19151"
            + " -thread_count 4"// -transaction_size 1 -batch_size 100"
            + " -aggregate_uri_id ArticleTitle"
            + " -input_file_type aggregates"
            + " -thread_count 1" //comment this line to reproduce
            + " -output_uri_replace \"\\[,'',\\],'',:,''\""
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
        assertEquals("1162", result.next().asString());
        Utils.closeSession();
    }
   
    @Test
    public void testEscapedQuoteInAtrr() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/21045.xml"
            + " -aggregate_record_element parent"
            + " -input_file_type aggregates"
            + " -output_uri_replace \"\\[,'',\\],'',:,''\""
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
        assertEquals("3", result.next().asString());
        Utils.closeSession();
    }
    
    /*
     * multithread mapper to load a file with 4 threads (thread_count is 4 by
     * default)
     */
    @Test
    public void testImportMedlineMultiWriter() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -aggregate_uri_id PMID"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineISO88591() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/encoding/medline04.small.iso-8859-1.xml -content_encoding iso-8859-1"
            + " -thread_count 1 -aggregate_uri_id PMID"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineISO88591Zip() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/encoding/medline04.small.iso-8859-1.zip -content_encoding iso-8859-1"
            + " -thread_count 1 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineZip() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04.zip"
            + " -thread_count 2 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed -input_compressed true"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMedlineZipUTF16() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04_utf16.zip"
            + " -content_encoding utf-16le"
            + " -thread_count 2 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed -input_compressed true"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformMedlineZip() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04.zip"
            + " -transform_module /lc_test.xqy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -thread_count 2 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed -input_compressed true"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformMedlineZipGenId() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04.2.zip"
            + " -transform_module /lc_test.xqy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -thread_count 2"
            + " -input_file_type aggregates -input_compressed -input_compressed true"
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
        assertEquals("4", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformMedlineZipFast() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/medlinezip/medline04.zip"
            + " -fastload"
            + " -transform_module /lc_test.xqy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -thread_count 2 -aggregate_uri_id PMID"
            + " -input_file_type aggregates -input_compressed -input_compressed true"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }
    
    @Test
    public void testBug19146() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -aggregate_record_element ArticleTitle"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testBug24908() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/24908.xml"
            + " -aggregate_record_element wpt"
            + " -aggregate_record_namespace http://www.topografix.com/GPX/1/0"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testIDWithNS() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/31691.xml"
            + " -aggregate_record_element item"
            + " -uri_id post_id -thread_count 1"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testIDWithNSZip() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/31691.zip"
            + " -aggregate_record_element item"
            + " -input_compressed"
            + " -uri_id post_id -thread_count 1"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
//    @Test
    public void testBadXML() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/bad.xml"
            + " -aggregate_record_element r"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportAggZip() throws Exception {
        String cmd = "IMPORT -host localhost" +
        		" -username admin" + " -password admin" + 
        		" -input_file_path " + Constants.TEST_PATH.toUri() + "/agg.zip" + 
        		" -aggregate_record_element p -aggregate_uri_id id" +
        		" -input_file_type aggregates -input_compressed true" +
        		" -input_compression_codec zip" +
        		" -namespace http://marklogic.com/foo"
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
        assertEquals("4", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath() + "/keys/TestImportAggregate#testImportAggZip.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportMedlineAutoID() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
            + " -thread_count 1"
            + " -input_file_type aggregates"
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
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }
}
