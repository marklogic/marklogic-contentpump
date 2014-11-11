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
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.rdf"
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
    public void testTransformRDFXML() throws Exception {
        Utils.prepareModule("xcc://admin:admin@localhost:5275", "/RDFAddNode.xqy");
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.rdf"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold
                        + " -transform_namespace http://marklogic.com/module_invoke"
                        + " -transform_param myparam"
                        + " -transform_module /RDFAddNode.xqy";
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
        assertEquals("459", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.daml.org/2001/12/factbook/vi#A113963\"])");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testTurtle() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.ttl"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("795", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/rdf-syntax-grammar\"])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testTurtlePermission() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.ttl"
                        + " -output_permissions admin,read,admin,update"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("795", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/rdf-syntax-grammar\"])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }    
 
    @Test
    public void testTurtlePermissionNonAdmin() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.ttl"
                        + " -output_permissions rest-admin,read,rest-admin,update"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("795", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/rdf-syntax-grammar\"])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(for $doc in fn:doc() return (xdmp:document-get-permissions(fn:base-uri($doc))))"
        );
        assertTrue(result.hasNext());
        assertEquals("18", result.next().asString());
        
        Utils.closeSession();
    }    
    
    @Test
    public void testRDFJSON() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.json"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("12", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://jondoe.example.org/#me\"])");
        assertTrue(result.hasNext());
        assertEquals("12", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testN3() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.n3"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("133", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("13257", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/TR/2004/REC-DOM-Level-3-Val-20040127\"])");
        assertTrue(result.hasNext());
        assertEquals("22", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
    
        Utils.closeSession();
    }

    @Test
    public void testNT() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nt"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("8", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://www.w3.org/2001/sw/RDFCore/ntriples/\"])");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testNT2013() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics-2013.nt"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples/sem:triple[sem:subject = \"http://one.example/subject1\"])");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:doc(\"http://marklogic.com/semantics#default-graph\"))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testTrig() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.trig"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(/sem:triples)");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://marklogic.com/semantics#default-graph')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G1')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G2')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G3')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("9", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
            + "fn:count(//sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
    
        Utils.closeSession();
    }

    @Test
    public void testTrig_my_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.trig"
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
        assertEquals("1", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://marklogic.com/semantics#default-graph')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G1')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G2')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://www.example.org/exampleDocument#G3')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
                + "fn:count(collection('http://marklogic.com/collection/1')//sem:triple)");
        assertTrue(result.hasNext());
        assertEquals("16", result.next().asString());

        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; "
            + "fn:count(//sem:graph)");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
    }

    @Test
    public void testNQ_quad_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
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
        assertEquals("2", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testNQ_my_coll() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/semantics.nq"
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
        assertEquals("21", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(fn:collection(\"http://en.wikipedia.org/wiki/Ani_DiFranco?oldid=490340130#absolute-line=1\"))");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        Utils.closeSession();
    }

    @Test
    public void testOutputURIPrefix() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/livesIn.ttl"
                        + " -output_uri_prefix /fred/"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; xdmp:node-uri((/sem:triples)[1])");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().startsWith("/fred/"));

        Utils.closeSession();
    }

    @Test
    public void testTypeAndLang() throws Exception {
        String cmd =
                "IMPORT -host localhost -port 5275 -username admin -password admin"
                        + " -input_file_path " + Constants.TEST_PATH.toUri() + "/bug24420.ttl"
                        + " -input_file_type rdf -rdf_streaming_memory_threshold " + threshold;

        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:object[@datatype])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:object[@xml:lang])");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        result = Utils.runQuery(
                "xcc://admin:admin@localhost:5275", "declare namespace sem=\"http://marklogic.com/semantics\"; fn:count(//sem:object[@datatype and @xml:lang])");
        assertTrue(result.hasNext());
        assertEquals("0", result.next().asString());

        Utils.closeSession();
    }

}
