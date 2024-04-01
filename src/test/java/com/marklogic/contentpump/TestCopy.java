package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;

public class TestCopy{

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
	@Test
	public void testBug20168() throws Exception {
		String cmd = "COPY -input_host localhost "
				+ "-input_username admin -input_password admin "
				+ "-output_host localhost -output_username admin "
				+ "-output_password admin -thread_count 1"
				+ " -input_port " + Constants.port + " -input_database " + Constants.testDb
				+ " -output_port " + Constants.port + " -output_database " + Constants.copyDst;

		String[] args = cmd.split(" ");

		Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
		ContentCreateOptions options = new ContentCreateOptions();
		options.setFormatXml();

		ContentSource cs = ContentSourceFactory.newContentSource(new URI(
				Utils.getTestDbXccUri()));
		Session session = cs.newSession();

		session.setTransactionMode(TransactionMode.UPDATE);
		Content content = ContentFactory.newContent("nocontent", new byte[0],
				0, 0, options);
		session.insertContent(content);
		byte[] str = "<r>some content</r>".getBytes();
		content = ContentFactory.newContent("hascontent", str, 0, str.length,
				options);
		session.insertContent(content);
		session.commit();
		session.close();

		Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);

		String[] expandedArgs = null;
		expandedArgs = OptionsFileUtil.expandArguments(args);
		ContentPump.runCommand(expandedArgs);
		
		ResultSequence result = Utils
				.runQuery(Utils.getTestDbXccUri(),
						"fn:count(fn:collection())");
		assertTrue(result.hasNext());
		assertEquals("2", result.next().asString());
		Utils.closeSession();
	}

    @Test
    public void testCopy() throws Exception {
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -output_uri_suffix .xml"
            + " -output_uri_prefix test/"
            + " -port " + Constants.port + " -database " + Constants.testDb;
            
        
        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //copy
        cmd = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -output_uri_suffix .suffix"
            + " -output_uri_prefix prefix/"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQueryAgainstDb(
                Utils.getTestDbXccUri(), "fn:doc()", Constants.copyDst);
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.endsWith(".xml.suffix"));
            assertTrue(uri.startsWith("prefix/test/"));
        }
        Utils.closeSession();
    }
    
    @Test
    public void testCopyWOProps() throws Exception {
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -output_uri_suffix .xml"
            + " -output_uri_prefix test/"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        
        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //copy
        cmd = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -output_uri_suffix .suffix"
            + " -output_uri_prefix prefix/"
            + " -copy_properties false"
            + " -batch_size 1"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQueryAgainstDb(
                Utils.getTestDbXccUri(), "fn:doc()", Constants.copyDst);
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.endsWith(".xml.suffix"));
            assertTrue(uri.startsWith("prefix/test/"));
        }
        Utils.closeSession();
    }
    
    @Test
    public void testCopyTransform() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -output_uri_suffix .xml -output_collections import"
            + " -output_uri_prefix test/"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        
        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //copy
        cmd = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -output_uri_suffix .suffix"
            + " -output_uri_prefix prefix/ -output_collections copy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /lc_test.xqy"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQueryAgainstDb(
                Utils.getTestDbXccUri(), "fn:doc()", Constants.copyDst);
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.endsWith(".xml.suffix"));
            assertTrue(uri.startsWith("prefix/test/"));
        }
        Utils.closeSession();
    }
    
    @Test
    public void testCopyTransformFast() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -output_uri_suffix .xml -output_collections import"
            + " -output_uri_prefix test/"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        
        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        //copy
        cmd = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -fastload"
            + " -output_uri_suffix .suffix"
            + " -output_uri_prefix prefix/ -output_collections copy"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /lc_test.xqy"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQueryAgainstDb(
                Utils.getTestDbXccUri(), "fn:doc()", Constants.copyDst);
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.endsWith(".xml.suffix"));
            assertTrue(uri.startsWith("prefix/test/"));
        }
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }
    
    
    @Test
    public void testCopyFast() throws Exception {
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -fastload true -batch_size 1"
            + " -input_file_type delimited_text"
            + " -output_uri_suffix .xml"
            + " -output_uri_prefix test/"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        
        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        AssignmentManager.getInstance().setInitialized(false);
        //copy
        cmd = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -fastload -batch_size 1"
            + " -output_uri_suffix .suffix"
            + " -output_uri_prefix prefix/"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQueryAgainstDb(
                Utils.getTestDbXccUri(), "fn:doc()", Constants.copyDst);
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.endsWith(".xml.suffix"));
            assertTrue(uri.startsWith("prefix/test/"));
        }
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }
    
    @Test
    public void testBug20059() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/20059"
            + " -output_uri_prefix test/"
            + " -output_collections test,ML"
            + " -output_uri_replace wiki,'wiki1'"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(),
            "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        //copy
        cmd = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -thread_count 1"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        Utils.closeSession();
    }
    
    @Test
    public void testCopyNakedProps() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/mixnakedzip"
            + " -batch_size 1"
            + " -input_file_type archive"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.setDirectoryCreation(Utils.getTestDbXccUri(), "automatic");
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(),
            "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        //copy
        cmd = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -thread_count 1"
            + " -output_uri_suffix .x"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), 
            "fn:count(" +
            "for $i in cts:search(xdmp:document-properties(),cts:not-query(cts:document-fragment-query(cts:and-query( () )))) \n" +
            "where fn:contains(fn:base-uri($i),\"\\.naked\") eq fn:false() \n" +
            "return fn:base-uri($i)" +
            ")");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        
        Utils.setDirectoryCreation(Utils.getTestDbXccUri(), "manual");
        Utils.closeSession();
    }
    
    @Test
    public void testCopyTemporalDoc() throws Exception {
        String cmd1 = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/temporal"
            + " -fastload false"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args1 = cmd1.split(" ");
        assertFalse(args1.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.copyDst);
        
        String[] expandedArgs1 = null;
        expandedArgs1 = OptionsFileUtil.expandArguments(args1);
        ContentPump.runCommand(expandedArgs1);
        
        String cmd2 = "COPY -input_host localhost"
            + " -input_username admin -input_password admin"
            + " -output_host localhost"
            + " -output_username admin -output_password admin"
            + " -temporal_collection mycollection"
            + " -input_port " + Constants.port + " -input_database " + Constants.testDb
            + " -output_port " + Constants.port + " -output_database " + Constants.copyDst;
        String[] args2 = cmd2.split(" ");
        assertFalse(args2.length == 0);
        
        String[] expandedArgs2 = null;
        expandedArgs2 = OptionsFileUtil.expandArguments(args2);
        ContentPump.runCommand(expandedArgs2);
        
        ResultSequence result = Utils.runQueryAgainstDb(
          Utils.getTestDbXccUri(),
          "fn:count(fn:collection(\"mycollection\"))", Constants.copyDst);
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());        

        Utils.closeSession();
    }
}