package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestExport {
    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testExportArchive() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text";
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //export
        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
            + " -output_file_path " + Constants.OUT_PATH.toUri()
            + " -output_type archive";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        //import it back
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        cmd = "import -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.OUT_PATH.toUri()
            + " -input_file_type archive";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testExportZip() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text";
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //export
        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
            + " -output_file_path " + Constants.OUT_PATH.toUri()
            + " -output_type document -compress";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        //import it back
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        cmd = "import -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.OUT_PATH.toUri()
            + " -input_file_type documents -document_type xml"
            + " -input_compressed true";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testExportZipUTF16() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text";
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //export
        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
            + " -content_encoding UTF-16"
            + " -output_file_path " + Constants.OUT_PATH.toUri()
            + " -output_type document -compress";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        //import it back
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        cmd = "import -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.OUT_PATH.toUri()
            + " -input_file_type documents -document_type xml"
            + " -content_encoding UTF-16"
            + " -input_compressed true";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:doc()/root/last[. eq \"ross\"]/text()");
        assertTrue(result.hasNext());
        assertEquals("ross", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testExportZipSystemEncoding() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text";
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //export
        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
            + " -content_encoding System"
            + " -output_file_path " + Constants.OUT_PATH.toUri()
            + " -output_type document -compress";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        //import it back
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        cmd = "import -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.OUT_PATH.toUri()
            + " -input_file_type documents -document_type xml"
            + " -content_encoding System"
            + " -input_compressed true";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:doc()/root/last[. eq \"ross\"]/text()");
        assertTrue(result.hasNext());
        assertEquals("ross", result.next().asString());
        Utils.closeSession();
    }   
    
    @Test
    public void testExportDocs() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text";
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //export
        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
            + " -output_file_path " + Constants.OUT_PATH.toUri()
            + " -output_type document";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        //import it back
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        cmd = "import -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.OUT_PATH.toUri();
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testExportUTF16Docs() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text";
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //export
        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
            + " -output_file_path " + Constants.OUT_PATH.toUri()
            + " -output_type document -content_encoding UTF-16";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        //import it back
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        cmd = "import -host localhost -port 5275 -username admin -password admin"
            + " -content_encoding UTF-16 -document_type xml"
            + " -input_file_path " + Constants.OUT_PATH.toUri();
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
    }
}