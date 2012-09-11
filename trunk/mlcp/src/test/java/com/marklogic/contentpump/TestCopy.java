package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;

public class TestCopy{

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testCopy() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -output_uri_suffix .xml"
            + " -output_uri_prefix test/";
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        Utils.clearDB("xcc://admin:admin@localhost:6275", "CopyDst");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //copy
        cmd = "COPY -input_host localhost -input_port 5275"
            + " -input_username admin -input_password admin"
            + " -output_host localhost -output_port 6275"
            + " -output_username admin -output_password admin";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:6275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQuery(
                "xcc://admin:admin@localhost:6275", "fn:doc()");
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.endsWith(".xml"));
            assertTrue(uri.startsWith("test/"));
        }
        Utils.closeSession();
    }
}