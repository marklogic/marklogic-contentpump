package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
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
		String cmd = "COPY -mode local -input_host localhost -input_port 5275 "
				+ "-input_username admin -input_password admin "
				+ "-output_host localhost -output_username admin "
				+ "-output_password admin -output_port 6275 -thread_count 1";

		String[] args = cmd.split(" ");

		Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
		ContentCreateOptions options = new ContentCreateOptions();
		options.setFormatXml();

		ContentSource cs = ContentSourceFactory.newContentSource(new URI(
				"xcc://admin:admin@localhost:5275"));
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

		Utils.clearDB("xcc://admin:admin@localhost:6275", "CopyDst");

		String[] expandedArgs = null;
		expandedArgs = OptionsFileUtil.expandArguments(args);
		ContentPump.runCommand(expandedArgs);
		
		ResultSequence result = Utils
				.runQuery("xcc://admin:admin@localhost:6275",
						"fn:count(fn:collection())");
		assertTrue(result.hasNext());
		assertEquals("2", result.next().asString());
		Utils.closeSession();
	}

    @Test
    public void testCopy() throws Exception {
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
            + " -output_username admin -output_password admin"
            + " -output_uri_suffix .suffix"
            + " -output_uri_prefix prefix/";
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
            assertTrue(uri.endsWith(".xml.suffix"));
            assertTrue(uri.startsWith("prefix/test/"));
        }
        Utils.closeSession();
    }
    
    @Test
    public void testCopyFast() throws Exception {
        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -fastload true"
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
            + " -output_username admin -output_password admin"
            + " -output_uri_suffix .suffix"
            + " -output_uri_prefix prefix/";
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
            assertTrue(uri.endsWith(".xml.suffix"));
            assertTrue(uri.startsWith("prefix/test/"));
        }
        Utils.closeSession();
    }
    
    @Test
    public void testBug20059() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/20059"
            + " -mode local -output_uri_prefix test/"
            + " -output_collections test,ML -port 5275"
            + " -output_uri_replace wiki,'wiki1'";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());
        Utils.closeSession();
        Utils.clearDB("xcc://admin:admin@localhost:6275", "CopyDst");
        
        //copy
        cmd = "COPY -input_host localhost -input_port 5275"
            + " -input_username admin -input_password admin"
            + " -output_host localhost -output_port 6275"
            + " -output_username admin -output_password admin"
            + " -thread_count 1"
            ;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:6275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("2", result.next().asString());

        Utils.closeSession();
    }
    
    @Test
    public void testCopyNakedProps() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/mixnakedzip"
            + " -input_file_type archive";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        Utils.setDirectoryCreation("xcc://admin:admin@localhost:5275", "automatic");
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275",
            "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        Utils.clearDB("xcc://admin:admin@localhost:6275", "CopyDst");
        
        //copy
        cmd = "COPY -input_host localhost -input_port 5275"
            + " -input_username admin -input_password admin"
            + " -output_host localhost -output_port 6275"
            + " -output_username admin -output_password admin"
            + " -thread_count 1"
            + " -output_uri_suffix .x"
            ;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:6275", 
            "fn:count(" +
            "for $i in cts:search(xdmp:document-properties(),cts:not-query(cts:document-fragment-query(cts:and-query( () )))) \n" +
            "where fn:contains(fn:base-uri($i),\"\\.naked\") eq fn:false() \n" +
            "return fn:base-uri($i)" +
            ")");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        Utils.closeSession();
        
        Utils.setDirectoryCreation("xcc://admin:admin@localhost:5275", "manual");
        Utils.closeSession();
    }
}