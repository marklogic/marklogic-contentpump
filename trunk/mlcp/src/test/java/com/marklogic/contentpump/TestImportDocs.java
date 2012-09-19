package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

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
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AbacuS"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
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
    public void testImportMixedDocsZip() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki.zip"
            + " -thread_count 1 -mode local"
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
    public void testImportDocsZipUTF16BE() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() 
            + "/encoding/ML-utf-16be.zip -content_encoding UTF-16BE"
            + " -thread_count 1 -mode local -document_type text"
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
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
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
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            sb.append(result.next().asString());
        }

        Utils.closeSession();
        Utils.writeFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportMixedUTF16LE.txt", sb);
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportMixedUTF16LE.txt");
        assertTrue(sb.toString().trim().equals(key));
    }
    
    @Test
    public void testImportMixedTxtUTF16LE() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/encoding/ML-utf-16le.txt"
            + " -thread_count 1 -mode local -content_encoding UTF-16LE"
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
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
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
