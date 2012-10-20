package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestImportSequenceFile {
    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportSequenceFile() throws Exception {
        String cmd = 
            "IMPORT -password admin -username admin -host localhost -port 5275"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/seqfile/file5.seq"
            + " -thread_count 1 -mode local"
            + " -input_file_type sequencefile --output_uri_prefix ABC"
            + " -sequencefile_key_class com.marklogic.contentpump.examples.SimpleSequenceFileKey"
            + " -sequencefile_value_class com.marklogic.contentpump.examples.SimpleSequenceFileValue"
            + " -sequencefile_value_type Text"; 
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
        
        result = Utils.getOnlyDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            sb.append(result.next().asString());
        }

        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportText#testImportSequenceFile.txt");
        assertTrue(sb.toString().trim().equals(key));
    }
}
