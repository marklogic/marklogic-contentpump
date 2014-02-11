package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;

public class TestImportDocs {
    
    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportMixedDocs() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -mode local -output_uri_prefix test/"
            + " -output_collections test,ML -port 5275"
            + " -fastload false"
            + " -output_uri_replace wiki,'wiki1'";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQuery("xcc://admin:admin@localhost:5275",
            "xdmp:directory(\"test/\", \"infinity\")");
        int count = 0;
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.contains("wiki1"));
            count++;
        }
        assertTrue(count == 93);
        Utils.closeSession();
    }
    
//    @Test
    public void testImportMixedDocsIncorrectHost() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhostx"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -mode local -output_uri_prefix test/"
            + " -output_collections test,ML -port 5275"
            + " -output_uri_replace wiki,'wiki1'";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQuery("xcc://admin:admin@localhost:5275",
            "xdmp:directory(\"test/\", \"infinity\")");
        int count = 0;
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.contains("wiki1"));
            count++;
        }
        assertTrue(count == 93);
        Utils.closeSession();
    }
    
    @Test
    public void testImportText() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AbacuS.xml"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -fastload"
            + " -output_collections test,ML -document_type text";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTextAsBinary() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AbacuS.xml"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -output_collections test,ML -document_type binary";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
    
     
    @Test
    public void testImportTransformMixed() throws Exception {
        Utils.prepareModule("xcc://admin:admin@localhost:5275", "/lc.xqy");
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"///AbacuS.xml"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -output_collections test,ML"
            + " -output_permissions admin,read,admin,update,admin,insert,admin,execute"
            + " -output_quality 1"
            + " -output_language fr"
            + " -namespace test"
            + " -fastload"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_function transform"
            + " -transform_module /lc.xqy"
            + " -transaction_size 10";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:doc()/node()[xdmp:node-kind(.) eq \"element\"])");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformBinary() throws Exception {
        Utils.prepareModule("xcc://admin:admin@localhost:5275", "/lc.xqy");
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/2012-06-13_16-26-58_431.jpg"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -output_collections test,ML -document_type binary"
            + " -output_permissions admin,read,admin,update,admin,insert,admin,execute"
            + " -output_quality 1"
            + " -output_language fr"
            + " -namespace test"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /lc.xqy";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransform25444() throws Exception {
        Utils.prepareModule("xcc://admin:admin@localhost:5275", "/trans.xqy");
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/foo.0"
            + " -transform_namespace dmc"
            + " -transform_module /trans.xqy";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportTransformText() throws Exception {
        Utils.prepareModule("xcc://admin:admin@localhost:5275", "/lc.xqy");
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AbacuS.xml"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -output_collections test,ML -document_type text"
            + " -output_permissions admin,read,admin,update,admin,insert,admin,execute"
            + " -output_quality 1"
            + " -output_language fr"
            + " -namespace test"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_param myparam"
            + " -transform_module /lc.xqy";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportXML() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -output_collections test,ML -document_type XML"
            + " -input_file_pattern ^A.*";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("89", result.next().asString());
        Utils.closeSession();
    }
    
//    @Test
//    public void testImportXMLExpectFailure() throws Exception {
//        ResultSequence result = Utils.runQuery(
//            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
//        assertTrue(result.hasNext());
//        assertEquals("-1", result.next().asString());
//        Utils.closeSession();
//    }
    
    @Test
    public void testImportXMLOutputDirNonfast() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -document_type XML"
            + " -output_directory /test -fastload false"
            + " -input_file_pattern ^A.*";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("89", result.next().asString());
        Utils.closeSession();
    }
    
    
    @Test
    public void testImportMixedDocsZip() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki.zip"
            + " -thread_count 4 -mode local"
            + " -input_compressed -input_compression_codec zip"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMixedDocsZipFast() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki.zip"
            + " -thread_count 4 -mode local -fastload"
            + " -input_compressed -input_compression_codec zip"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportMixedDocsZipHTTP() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki.zip" //"/space/tmp/cpox/tmp/WikiToZip-00000080.zip"
            + " -thread_count 4 -mode local"
            + " -input_compressed -input_compression_codec zip"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
    }
    
    
    /*
     * ingest two zip files
     * one loaded with MultithreadedMapper using 2 threads
     * one loaded with DocumentMapper
     */
    @Test
    public void testImportMixedDocsZipMultithreadedMapper() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/zips" 
            + " -thread_count 2 -mode local"
            + " -input_compressed -input_compression_codec zip"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("186", result.next().asString());
        Utils.closeSession();
    }
    
    /*
     * ingest two zip files
     * each loaded with MultithreadedMapper using 2 threads
     * thread_count is 3
     */
    @Test
    public void testImportMixedDocsZipMultithreadedMapper2() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/zips" 
            + " -thread_count 3 -mode local"
            + " -input_compressed -input_compression_codec zip"
            + " -output_collections test,ML" 
            + " -thread_count_per_split 2";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("186", result.next().asString());
        Utils.closeSession();
    }
    
    /*
     * ingest two zip files
     * each loaded with MultithreadedMapper using 1 threads
     * use thread_count_per_split =1 to enforce old behavior
     * thread_count is 3
     */
    @Test
    public void testImportMixedDocsZipMultithreadedMapperOldBehavior() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/zips" 
            + " -thread_count 3 -mode local"
            + " -input_compressed -input_compression_codec zip"
            + " -output_collections test,ML" 
            + " -thread_count_per_split 1";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("186", result.next().asString());
        Utils.closeSession();
    }
    
    /*
     * ingest two zip files
     * each loaded with MultithreadedMapper using 2 threads
     * 
     */
    @Test
    public void testImportMixedDocsZipMultithreadedMapper3() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/zips" 
            + " -mode local"
            + " -input_compressed -input_compression_codec zip"
            + " -output_collections test,ML" 
            + " -thread_count_per_split 2";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("186", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testImportDocsZipUTF16BE() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() 
            + "/encoding/ML-utf-16be.zip -content_encoding UTF-16BE"
            + " -thread_count 1 -mode local -document_type text"
            + " -input_compressed -input_compression_codec zip"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            sb.append(result.next().asString());
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportDocsZipUTF16BE.txt");
        assertTrue(sb.toString().trim().equals(key));
    }

    @Test
    public void testImportTextUTF16LE() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-16le.enc"
            + " -thread_count 1 -mode local -content_encoding UTF-16LE"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -output_collections test,ML -document_type text";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            sb.append(result.next().asString());
        }
        Utils.closeSession();
        
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportTextUTF16LE.txt");
        assertTrue(sb.toString().trim().equals(key));

    }
    
    @Test
    public void testImportMixedUTF16LE() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-16le.enc"
            + " -thread_count 1 -mode local -content_encoding UTF-16LE"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");

        assertTrue(result.hasNext());
        InputStream is = result.next().asInputStream();
        String str = getResult(is, "UTF-8");

        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportMixedUTF16LE.txt");
        assertTrue(str.trim().equals(key));
    }

    @Test
    public void testImportMixedUTF8() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-8.enc"
            + " -thread_count 1 -mode local"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");

        assertTrue(result.hasNext());
        InputStream is = result.next().asInputStream();
        //data in server is UTF-8 encoded
        String str = getResult(is, "UTF-8");

        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportMixedUTF8.txt");
        assertTrue(str.trim().equals(key));
    }
    
    @Test
    public void testImportTxtUTF8() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-8.enc"
            + " -thread_count 1 -mode local -document_type TEXT"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");

        assertTrue(result.hasNext());
        InputStream is = result.next().asInputStream();
        //data in server is UTF-8 encoded
        String str = getResult(is, "UTF-8");

        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportTxtUTF8.txt");
        assertTrue(str.trim().equals(key));
    }
//    @Test
//    public void testImportMixedTxtUTF8LE_bad() throws Exception {
//        String cmd = 
//            "IMPORT -password admin -username admin -host localhost -port 5275"
//            + " -input_file_path " + "/space2/qa/mlcp/data/xml/bigdata/doc108.xml"// + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-8.txt"
//            + " -thread_count 1 -mode local"
//            + " -content_encoding utf-8le"
//            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
//            + " -output_collections test,ML";
//        String[] args = cmd.split(" ");
//        assertFalse(args.length == 0);
//
//        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
//
//        String[] expandedArgs = null;
//        expandedArgs = OptionsFileUtil.expandArguments(args);
//        ContentPump.runCommand(expandedArgs);
//
//        ResultSequence result = Utils.runQuery(
//            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
//        assertTrue(result.hasNext());
//        assertEquals("1", result.next().asString());
//        Utils.closeSession();
//        
//        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");
//
//        assertTrue(result.hasNext());
//        InputStream is = result.next().asInputStream();
//        //data in server is UTF-8 encoded
//        String str = getResult(is, "UTF-8");
//
//        Utils.closeSession();
//
//        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
//            + "/keys/TestImportText#testImportTxtUTF8.txt");
//        assertTrue(str.trim().equals(key));
//    }
    
    @Test
    public void testImportMixedTxtUTF8() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-8.txt"
            + " -thread_count 1 -mode local"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");

        assertTrue(result.hasNext());
        InputStream is = result.next().asInputStream();
        //data in server is UTF-8 encoded
        String str = getResult(is, "UTF-8");

        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportTxtUTF8.txt");
        assertTrue(str.trim().equals(key));
    }
    
    private String getResult(InputStream is, String encoding) throws IOException {
        return getResultSB(is,encoding).toString();
    }
    
    private StringBuilder getResultSB(InputStream is, String encoding) throws IOException {
        StringBuilder sb = new StringBuilder();
        InputStreamReader isr = new InputStreamReader(is, encoding);
        char [] buffer = new char[65535];
        int num = -1;
        while((num = isr.read(buffer))!=-1) {
            sb.append(buffer,0, num);
        }
        return sb;
    }
    
//    @Test
    public void testImportBug20697() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + "/space2/qa/mlcp/data/xml/bigdata"
            + " -input_file_pattern doc108.*"
            + " -thread_count 1 -mode local -content_encoding UTF-8LE"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            sb.append(result.next().asString());
        }

        Utils.closeSession();

//        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
//            + "/keys/TestImportText#testImportMixedUTF16LE.txt");
//        assertTrue(sb.toString().trim().equals(key));
    }
    
    @Test
    public void testImportMixedTxtUTF16LE() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-16le.txt"
            + " -thread_count 1 -mode local -content_encoding UTF-16LE"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -output_collections test,ML";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            sb.append(result.next().asString());
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportMixedTxtUTF16LE.txt");
        assertTrue(sb.toString().trim().equals(key));

    }
 
}
