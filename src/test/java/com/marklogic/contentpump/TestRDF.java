package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(value = Parameterized.class)
public class TestRDF {
    private long threshold = 0;

    public TestRDF(long threshold) {
        this.threshold = threshold;
    }

    @Parameterized.Parameters
    public static java.util.Collection<Object[]> data() {
        Object[][] data = new Object[][] { { 64*1024*1000 }, { 0 } };
        // data = new Object[][] { { 0 } };
        return Arrays.asList(data);
    }

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testRDFXML() throws Exception {
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.rdf"
            + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
            + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("454", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.daml.org/2001/12/factbook/vi#A113963\"])");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testTransformRDFXML() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/RDFAddNode.xqy");
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.rdf"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -transform_namespace http://marklogic.com/module_invoke"
                        + " -transform_param myparam"
                        + " -transform_module /RDFAddNode.xqy"
                        + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("459", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.daml.org/2001/12/factbook/vi#A113963\"])");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testTurtle() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.ttl"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("795", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/rdf-syntax-grammar\"])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testNBDCTurtle() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/NBDC_sample.ttl"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("12", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("1171", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        Utils.closeSession();
    }
    @Test
    public void testTurtlePermission() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.ttl"
                        + " -output_permissions admin,read,admin,update"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("795", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/rdf-syntax-grammar\"])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }    
 
//    @Test
    public void testTurtlePermissionNonAdmin() throws Exception {
        String cmd =
                "IMPORT -host localhost -username sparqlupdate -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.ttl"
                        + " -output_collections http://marklogic.com/semantics/sb/customers/FT"
                        + " -output_permissions sparql-update-user,read"
//                        + " -thread_count 1"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

//        Utils.clearDB(Utils.getDocumentsDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("795", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/rdf-syntax-grammar\"])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(for $doc in fn:doc() return (xdmp:document-get-permissions(fn:base-uri($doc))))"
        );
        assertTrue(result.hasNext());
        assertEquals("18", result.next().asString());
        
        Utils.closeSession();
    }    
    
    @Test
    public void testRDFJSON() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.json"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("12", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://jondoe.example.org/#me\"])");
        assertTrue(result.hasNext());
        assertEquals("12", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testN3() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.n3"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("133", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("13257", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/2004/REC-DOM-Level-3-Val-20040127\"])");
        assertTrue(result.hasNext());
        assertEquals("22", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
    
        Utils.closeSession();
    }
    
    @Test
    public void testN3_output_graph() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.n3"
                        + " -output_graph http://myDefaultGraph.com"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("133", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("13257", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://myDefaultGraph.com\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("13257", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/2004/REC-DOM-Level-3-Val-20040127\"])");
        assertTrue(result.hasNext());
        assertEquals("22", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://myDefaultGraph.com\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
    
        Utils.closeSession();
    }
    
    /**
     * Although Jena parser tries its best to parse the corrupted file, 
     * it's not guranteed to read the good data from the currupted file. 
     * 
     * We make sure we at least load all data from the good files.
     */
    @Test
    public void test34887() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/34887"
                        + " -output_graph http://myDefaultGraph.com"
                        + " -thread_count 1"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        Utils.closeSession();
        
        ResultSequence result = Utils
            .runQuery(
                Utils.getTestDbXccUri(),
                "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        int tmp = Integer.parseInt(result.next().asString());
        assertTrue(tmp == 2 || tmp == 3);

        result = Utils
            .runQuery(
                Utils.getTestDbXccUri(),
                "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        tmp = Integer.parseInt(result.next().asString());
        assertTrue(tmp == 2 || tmp == 3);

        result = Utils
            .runQuery(
                Utils.getTestDbXccUri(),
                "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Autism?oldid=495234324#absolute-line=9\")//sem:triple)");
        assertTrue(result.hasNext());
        tmp = Integer.parseInt(result.next().asString());
        assertTrue(tmp == 1 || tmp == 2);

        result = Utils
            .runQuery(
                Utils.getTestDbXccUri(),
                "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://myDefaultGraph.com\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
    
        Utils.closeSession();
    }
    
    @Test
    public void testN3_override_graph() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.n3"
                        + " -output_override_graph http://myDefaultGraph.com"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("133", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("13257", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://myDefaultGraph.com\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("13257", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/2004/REC-DOM-Level-3-Val-20040127\"])");
        assertTrue(result.hasNext());
        assertEquals("22", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://myDefaultGraph.com\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
    
        Utils.closeSession();
    }

    @Test
    public void testNT() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nt"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/2001/sw/RDFCore/ntriples/\"])");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testNT2013() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics-2013.nt"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://one.example/subject1\"])");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testTrig() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.trig"
                        + " -output_uri_prefix /xyz"
                        + " -fastload false"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://marklogic.com/semantics#default-graph')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G1')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G2')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G3')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("9", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
            + "fn:count(//sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
    
        Utils.closeSession();
    }

    @Test
    public void testTrig_my_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.trig"
                        + " -output_collections http://marklogic.com/collection/1"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://marklogic.com/semantics#default-graph')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G1')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G2')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G3')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://marklogic.com/collection/1')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; "
            + "fn:count(//sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testNQ_quad_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("343", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://marklogic.com/collection/1\"))");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Ani_DiFranco?oldid=490340130#absolute-line=1\"))");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://marklogic.com/semantics#default-graph\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        
        Utils.closeSession();
    }
    
//    @Test
    public void testNQ_quad_performance() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + "/space/Downloads/root_support_bugtracking_attachments_30886_homepages_en.nq"
                        + " -output_permissions admin,read,admin,update"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
//
        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("50", result.next().asString());

        
        cmd =
            "IMPORT -host localhost -username admin -password admin"
                    + " -input_file_path " + "/space/Downloads/root_support_bugtracking_attachments_30886_homepages_en.nq"
                    + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                    + " -port " + Constants.port + " -database " + Constants.testDb;
        args = cmd.split(" ");

        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils
            .runQuery(
                Utils.getTestDbXccUri(),
                "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("100", result.next().asString());

        result = Utils
            .runQuery(
                Utils.getTestDbXccUri(),
                "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("50", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testNQ_my_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
                        + " -output_collections http://marklogic.com/collection/1"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("21", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://marklogic.com/collection/1\"))");
        assertTrue(result.hasNext());
        assertEquals("22", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Ani_DiFranco?oldid=490340130#absolute-line=1\"))");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        Utils.closeSession();
    }
    
    @Test
    public void testNQ_override_graph() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
                        + " -output_override_graph http://myOverrideGraph.com"
//                        + " -output_graph xyz" //negative test
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("21", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());


        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Autism?oldid=495234324#absolute-line=9\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://myOverrideGraph.com\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());

        Utils.closeSession();
    }
    
    @Test
    public void testNQ_override_graph1() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
                        + " -output_override_graph http://myOverrideGraph.com"
                        + " -output_collections a,b,c" 
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("21", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Autism?oldid=495234324#absolute-line=9\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://myOverrideGraph.com\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"b\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());


        Utils.closeSession();
    }
    
    @Test
    public void testNQ_my_default_graph() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
                        + " -output_graph http://mydefaultgraph.com"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("343", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());


        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Autism?oldid=495234324#absolute-line=9\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://mydefaultgraph.com\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        Utils.closeSession();
    }
    
    @Test
    public void testNQ_my_default_graph1() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
                        + " -output_graph http://mydefaultgraph.com"
                        + " -output_collections a,b,c"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("343", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2001", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://dbpedia.org/resource/Animal_Farm\"])");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());


        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Autism?oldid=495234324#absolute-line=9\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://mydefaultgraph.com\")//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testOutputURIPrefix() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/livesIn.ttl"
                        + " -output_uri_prefix /fred/"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; xdmp:node-uri((/sem:triples)[1])");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().startsWith("/fred/"));

        Utils.closeSession();
    }

    @Test
    public void testTypeAndLang() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/bug24420.ttl"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -port " + Constants.port + " -database " + Constants.testDb;

        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:object[@datatype])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:object[@xml:lang])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
                Utils.getTestDbXccUri(), "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:object[@datatype and @xml:lang])");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        Utils.closeSession();
    }

}
