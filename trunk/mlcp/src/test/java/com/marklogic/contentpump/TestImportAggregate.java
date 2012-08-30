package com.marklogic.contentpump;

import junit.framework.TestCase;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestImportAggregate extends TestCase {
    public TestImportAggregate(String name) {
        super(name);
    }
    

    public void testImportMedline() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/agg/medline04.small.xml"
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
    }
    

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
    }
    

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
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath() + "/keys/TestImportAggregate#testImportAggZip.txt");
        assertTrue(sb.toString().equals(key));
    }
    

}
