package com.marklogic.contentpump;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class TestCompressedRDF {
    private long threshold = 0;

    public TestCompressedRDF(long threshold) {
        this.threshold = threshold;
    }

    @Parameterized.Parameters
    public static java.util.Collection<Object[]> data() {
        Object[][] data = new Object[][] { { 0 }, { 64*1024*1000 } };
        return Arrays.asList(data);
    }

    @After
    public void tearDown() {
        Utils.closeSession();
    }

    @Test
    public void testGZipRDFXML() throws Exception {
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_compressed true -input_compression_codec gzip"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/c-semantics.rdf.gz"
            + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("454", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.daml.org/2001/12/factbook/vi#A113963\"])");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testZip_quad_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_compressed true -input_compression_codec zip"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/c-semantics.zip"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("491", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16528", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.daml.org/2001/12/factbook/vi#A113963\"])");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testZip_my_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_compressed true -input_compression_codec zip"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/c-semantics.zip"
                        + " -output_collections http://marklogic.com/collection/1"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("169", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16528", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.daml.org/2001/12/factbook/vi#A113963\"])");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testGZipNQ_quad_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_compressed true -input_compression_codec gzip"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/c-semantics.nq.gz"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("342", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("1999", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://marklogic.com/collection/1\"))");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Ani_DiFranco?oldid=490340130#absolute-line=1\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testGZipNQ_my_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_compressed true -input_compression_codec gzip"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/c-semantics.nq.gz"
                        + " -output_collections http://marklogic.com/collection/1"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("20", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("1999", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://marklogic.com/collection/1\"))");
        assertTrue(result.hasNext());
        assertEquals("20", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Ani_DiFranco?oldid=490340130#absolute-line=1\"))");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        Utils.closeSession();
    }
}