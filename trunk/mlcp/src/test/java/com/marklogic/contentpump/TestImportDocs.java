package com.marklogic.contentpump;

import junit.framework.TestCase;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestImportDocs extends TestCase {
     public TestImportDocs(String name) {
        super(name);
    }
    
    public void testImportMixedDocs() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -thread_count 1 -mode local -output_uri_prefix ABC"
            + " -output_collections test,ML -port 5275";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
    }
    
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
    }
    
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
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection(\"test\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
    }
 
}
