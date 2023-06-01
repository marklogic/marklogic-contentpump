package com.marklogic.contentpump;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.ResultSequence;

public class TestImportDelimitedText{

    @After
    public void tearDown() {
        Utils.closeSession();
    }

    @Test
    public void testImportDelimitedText_() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample1.quote.csv"
            + " -fastload"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
//        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
//            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
//        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedText() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextGenerateId() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -generate_uri"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedGenerateId.txt");
        System.out.println("DUMP:" + cmd);
        System.out.println("DUMP:" + sb.length() +"@" + sb.toString());
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextRootName() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimited_root_name rot"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(/rot)");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testImportTransformDelimitedText() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /lc_test.xqy"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportTransformDelimitedTextFileNameAsCollection() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /lc_test.xqy"
            + " -filename_as_collection true"
            + " -delimited_uri_id first"
            + " -thread_count 1"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(cts:collections())");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportTransformOne2ManyDelimitedText() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/one-many.xqy");
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /one-many.xqy"
            + " -delimited_uri_id first"
            + " -transform_param -param-up"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("14", result.next().asString());
        Utils.closeSession();
//
//        result = Utils.getNonEmptyDocsURIs(Utils.getDocumentsDbXccUri());
//
//        StringBuilder sb = new StringBuilder();
//        while(result.hasNext()) {
//            String s = result.next().asString();
//            sb.append(s);
//        }
//        Utils.closeSession();
//        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
//            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
//        assertTrue(sb.toString().equals(key));
    }

/*
 *  test output_language and namespace
 */

    @Test
    public void testImportTransformDelimitedTextLanNs() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -output_language fr"
            + " -namespace test"
            + " -transform_module /lc_test.xqy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        Utils.closeSession();

//        result = Utils.getNonEmptyDocsURIs(Utils.getDocumentsDbXccUri());
//
//        StringBuilder sb = new StringBuilder();
//        while(result.hasNext()) {
//            String s = result.next().asString();
//            sb.append(s);
//        }
//        Utils.closeSession();
//        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
//            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
//        assertTrue(sb.toString().equals(key));
    }
    @Test
    public void testImportDelimitedTextPipe() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -generate_uri -delimiter |"
            + " -input_file_type delimited_text -input_file_pattern .*\\.tpch -thread_count 1"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
    }

    //split each file into 3 splits, the last of which is skipped bc' reaching the end
    @Test
    public void testImport3DelimitedSplits() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimited_uri_id NAME -delimiter |"
            + " -split_input true"
            + " -max_split_size 200"
            + " -input_file_type delimited_text -input_file_pattern .*\\.tpch -thread_count 1"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testImport2DelimitedSplits() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimiter |"
            + " -split_input"
            + " -generate_uri"
            + " -max_split_size 300"
            + " -input_file_type delimited_text -input_file_pattern .*\\.tpch -thread_count 1"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testImportDelimitedTextTab() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimited_uri_id NAME -delimiter \t"
            + " -input_file_type delimited_text -input_file_pattern .*\\.tab -thread_count 1"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testImportDelimitedTextPipe2() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/1.pipe"
            + " -delimited_uri_id CUSTKEY -delimiter |"
            + " -output_uri_prefix /incoming/site-catalyst/total/"
            + " -output_uri_replace /home/marklogic/mlcp/marklogic-contentpump-1.0.3/data,''"
            + " -input_file_type delimited_text -thread_count 1"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testImportDelimitedTextBad() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample3.csv.bad"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testImportDelimitedTextElemNames() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample3.csv.ename"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextElemNames.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextElemNamesSplit() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample3.csv.ename"
            + " -split_input -max_split_size 50"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextElemNamesSplit.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextWithQuotes() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample.quote.csv"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testImportDelimitedTextHard() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample3.csv.hard"
            + " -delimited_uri_id first"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextHard.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextHardZip() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv/sample3.csv.hard.zip"
            + " -delimited_uri_id first -input_compressed"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextHardZip.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextUTF16LE() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri()
            + "/encoding/samplecsv.utf16le.csv -content_encoding utf-16le"
            + " -delimited_uri_id first"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextUTF16LE.txt");
        System.out.println("DUMP:" + sb.length() +"@" + sb.toString());
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextUTF16BE() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri()
            + "/encoding/samplecsv.utf16be.csv -content_encoding utf-16be"
            + " -delimited_uri_id first "
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextUTF16BE.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextUTF16LEZip() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri()
            + "/encoding/samplecsv.utf16le.zip -content_encoding utf-16le"
            + " -delimited_uri_id first -input_compressed"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextUTF16LEZip.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextZip() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextZip.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextZipGenId() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -generate_uri true"
            + " -input_compressed -input_compression_codec zip"
            + " -output_uri_replace " + Constants.MLCP_HOME + ",'/space/workspace/xcc/mlcp'"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedTextZipGenId.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportTransformDelimitedTextZip() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -transform_module /lc_test.xqy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());
        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();
        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportTransformDelimitedTextZip.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextDocJSON() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path "
                + Constants.TEST_PATH.toUri()
                + "/csv"
                + " -input_file_type delimited_text -input_file_pattern .*\\.csv"
                + " -document_type json"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(Utils.getTestDbXccUri(),
                        "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        Utils.closeSession();

        result = Utils.assertDocsFormat(Utils.getTestDbXccUri(),"JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
    }

    @Test
    public void testImportDelimitedTextDocJSONWithOptions() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path "
                + Constants.TEST_PATH.toUri()
                + "/csv"
                + " -input_file_type delimited_text -input_file_pattern sample.+\\.csv"
                + " -document_type json -delimited_root_name doc"
                + " -delimited_uri_id first -split_input true"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(Utils.getTestDbXccUri(),
                        "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
        Utils.closeSession();

        result = Utils.assertDocsFormat(Utils.getTestDbXccUri(),"JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }

        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedText#testImportDelimitedTextDocJSONWithOptions.txt");

        System.out.println(sb.toString());
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextJSONDataType() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path "
                + Constants.TEST_PATH.toUri()
                + "/csv/sample4.txt"
                + " -input_file_type delimited_text -data_type zipcode,String,score,number"
                + " -document_type json"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(Utils.getTestDbXccUri(),
                        "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }

        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedText#testImportDelimitedTextJSONDataType.txt");

        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testImportDelimitedTextInvalidType() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path "
                + Constants.TEST_PATH.toUri()
                + "/csv/1.input"
                + " -input_file_type delimited_text -data_type soldOut,Boolean,price,Number -uri_id name"
                + " -document_type json"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(Utils.getTestDbXccUri(),
                        "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();

        result = Utils.getAllDocs(Utils.getTestDbXccUri());
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }

        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedText#testImportDelimitedTextInvalidType.txt");

        assertTrue(sb.toString().equals(key));
    }

    //4 files into 6 delimited splits
    @Test
    public void testImportDelimitedTextSplit() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv"
            + " -delimited_uri_id first"
            + " -split_input -max_split_size 50"
            + " -input_file_type delimited_text"
            + " -input_file_pattern .*\\.csv"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("7", result.next().asString());
        Utils.closeSession();

        result = Utils.getNonEmptyDocsURIs(Utils.getTestDbXccUri());

        StringBuilder sb = new StringBuilder();
        while(result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        Utils.closeSession();

        String key = Utils.readSmallFile(Constants.TEST_PATH.toUri().getPath()
            + "/keys/TestImportDelimitedText#testImportDelimitedText.txt");
        assertTrue(sb.toString().equals(key));
    }

    @Test
    public void testBug44422() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedText/44422.csv"
                + " -fastload false -input_file_type delimited_text"
                + " -split_input true -max_split_size 1000 -uri_id id"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("900", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testBug42027() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedText/42027.csv"
                + " -fastload false -input_file_type delimited_text -document_type json"
                + " -split_input true -max_split_size 50 -generate_uri true"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1000", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testBug39564() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedText/39564.csv"
                + " -fastload false -input_file_type delimited_text"
                + " -split_input true -max_split_size 300 -generate_uri true"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("20", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testBug44422JP() throws Exception {
        String cmd =
                "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedText/44422JP.csv"
                + " -input_file_type delimited_text -split_input true -max_split_size 1000"
                + " -uri_id id -output_uri_prefix .xml"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("900", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testBug55944TwoBytesSmall() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri()
                + "/delimitedText/55944_two_bytes_small.csv"
                + " -input_file_type delimited_text -split_input true"
                + " -max_split_size 1000 -uri_id id -output_uri_prefix .xml"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertNotEquals(0, args.length);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("4", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testBug55944TwoBytes() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri()
                + "/delimitedText/55944_two_bytes.csv"
                + " -input_file_type delimited_text -split_input true"
                + " -max_split_size 1000 -uri_id id -output_uri_prefix .xml"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertNotEquals(0, args.length);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("100000", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testBug55944FourBytes() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri()
                + "/delimitedText/55944_four_bytes.csv"
                + " -input_file_type delimited_text -split_input true"
                + " -max_split_size 1000 -uri_id id -output_uri_prefix .xml"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertNotEquals(0, args.length);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1000", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testBug55944MixedBytes() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri()
                + "/delimitedText/55944_mixed_bytes.csv"
                + " -input_file_type delimited_text -split_input true"
                + " -max_split_size 1000 -uri_id id -output_uri_prefix .xml"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertNotEquals(0, args.length);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1000", result.next().asString());
        Utils.closeSession();
    }

    @Test
    // Test multi-byte delimiter
    public void testBug55944Delimiter() throws Exception {
        String cmd =
            "IMPORT -host localhost -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri()
                + "/delimitedText/55944_delimiter.csv -delimiter ，"
                + " -input_file_type delimited_text -split_input true"
                + " -max_split_size 1000 -uri_id id -output_uri_prefix .xml"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertNotEquals(0, args.length);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("10000", result.next().asString());
        Utils.closeSession();
    }
}
