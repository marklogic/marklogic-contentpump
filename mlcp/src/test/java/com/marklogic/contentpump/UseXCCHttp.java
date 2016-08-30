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
import com.marklogic.xcc.impl.ModuleImpl;

public class UseXCCHttp{

    @After
    public void tearDown() {
        Utils.closeSession();
        System.setProperty("xcc.httpcompliant", "false");
    }
    
    //@Test
    public void testImportMixedDocs() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -output_uri_prefix test/"
            + " -output_collections test,ML -port 5275"
            + " -output_uri_replace wiki,'wiki1' -thread_count 1"
            + " -batch_size 10";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getDbXccUri(), "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            Utils.getDbXccUri(),
            "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQuery(Utils.getDbXccUri(),
            "xdmp:directory(\"test/\", \"infinity\")");
        int count = 0;
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.contains("wiki1"));
            count++;
        }
        assertTrue(count == 93);
        Utils.closeSession();
    }
    
    //@Test
    public void testImportMixedDocsProxy() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
        String cmd = "IMPORT -password admin -username admin -host localhost"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
            + " -output_uri_prefix test/"
            + " -output_collections test,ML -port 80"
            + " -output_uri_replace wiki,'wiki1' -thread_count 1"
            + " -batch_size 10";
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB("xcc://admin:admin@localhost:80", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:80",
            "fn:count(fn:collection(\"ML\"))");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
        
        result = Utils.runQuery("xcc://admin:admin@localhost:80",
            "xdmp:directory(\"test/\", \"infinity\")");
        int count = 0;
        while (result.hasNext()) {
            ResultItem item = result.next();
            String uri = item.getDocumentURI();
            assertTrue(uri.contains("wiki1"));
            count++;
        }
        assertTrue(count == 93);
        Utils.closeSession();
    }
    
    /*
     * test using old xcc so that server catchs XDMP-RVSLNOTHTTP and rollback
     */
    //@Test
    public void testHTTPRollback() throws Exception {
//        System.setProperty("xcc.httpcompliant", "true");
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();

        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                Utils.getDbXccUri()));
      try (Session session = cs.newSession()) {
        session.setTransactionMode(TransactionMode.UPDATE);
        Content content = ContentFactory.newContent("nocontent", new byte[0],
                0, 0, options);
        session.insertContent(content);
        
        System.setProperty("xcc.httpcompliant", "true");
        
        byte[] str = "<r>some content</r>".getBytes();
        ContentCreateOptions options1 = new ContentCreateOptions();
        options1.setFormatXml();
        options1.setResolveEntities(true);
        content = ContentFactory.newContent("hascontent", str, 0, str.length,
                options1);
        session.insertContent(content);
        session.commit();
      }
    }
    
    //@Test
    public void testHTTPEval() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
        Utils.runQuery("xcc://admin:admin@localhost:8686", "\"Hello\"");
    }
    
    //@Test
    public void testXDBCEval() throws Exception {
        System.setProperty("xcc.httpcompliant", "false");
        Utils.runQuery(Utils.getDbXccUri(), "\"Hello\"");
    }
    
    //@Test
    public void testHTTPEvalProxy() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
        Utils.runQuery("xcc://admin:admin@ali:80", "\"Hello\"");
    }
    
    //@Test
    public void testXDBCEvalProxy() throws Exception {
        System.setProperty("xcc.httpcompliant", "false");
        Utils.runQuery("xcc://admin:admin@ali:80", "\"Hello\"");
    }
    
    //@Test
    public void testXDBCInvoke() throws Exception {
        System.setProperty("xcc.httpcompliant", "false");
//        Utils.runQuery(Utils.getDocumentsDbXccUri(), "\"Hello\"");
        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
        Utils.getDbXccUri()));
        Session session = cs.newSession();
        ModuleImpl request = (ModuleImpl) session.newModuleInvoke("/foo.xqy");

        request.setNewStringVariable("foo", "bar");
//        request.setOldEncodingStyle(true);
        session.submitRequest(request);
    }
    
    //@Test
    public void testHTTPChunkingEncoding() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();

        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                "xcc://admin:admin@:5275"));
      try (Session session = cs.newSession()) {
        session.setTransactionMode(TransactionMode.UPDATE);
        Content content = ContentFactory.newContent("nocontent", new byte[0],
                0, 0, options);
        session.insertContent(content);
        byte[] str = "<r>some content</r>".getBytes();
        content = ContentFactory.newContent("hascontent", str, 0, str.length,
                options);
        session.insertContent(content);
        session.commit();
      }
    }
    
    
    //@Test
    public void testHTTPChunkingEncodingProxySingleStmt() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();

        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                "xcc://admin:admin@ali:80"));
      try (Session session = cs.newSession()) {
        Content content = ContentFactory.newContent("nocontent", new byte[0],
                0, 0, options);
        session.insertContent(content);
      }
    }
    //@Test
    public void testHTTPChunkingEncodingProxy() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();

        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                "xcc://admin:admin@ali:80"));
      try (Session session = cs.newSession()) {
        session.setTransactionMode(TransactionMode.UPDATE);
        Content content = ContentFactory.newContent("nocontent", new byte[0],
                0, 0, options);
        session.insertContent(content);
        byte[] str = "<r>some content</r>".getBytes();
        content = ContentFactory.newContent("hascontent", str, 0, str.length,
                options);
        session.insertContent(content);
        session.commit();
      }
    }
    
    //@Test
    public void testHTTPMultiChunks() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        
        
        System.out.println(getClass().getClassLoader().getResource("xcc.properties"));
        System.out.println(System.getProperty("xcc.httpcompliant"));
//        Properties props = System.getProperties();//new Properties();
//        FileInputStream fis = new FileInputStream(getClass().getClassLoader().getResource("xcc.properties").getPath());
//        FileInputStream fis2 = new FileInputStream(getClass().getClassLoader().getResource("xcc.logging.properties").getPath());
//        props.load(new InputStreamReader(fis));
//        props.load(new InputStreamReader(fis2));
//        props.load
//        System.setProperties(props);
        
        
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();

        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                Utils.getDbXccUri()));
        
        System.out.println(System.getProperty("xcc.httpcompliant"));
        
      try (Session session = cs.newSession()) {
        session.setTransactionMode(TransactionMode.UPDATE);
        Content content;
        byte[] str = getLargeString().getBytes();
        content = ContentFactory.newContent("hascontent", str, 0, str.length,
                options);
        session.insertContent(content);
        session.commit();
      }
        
        ResultSequence result = Utils.runQuery(
            Utils.getDbXccUri(),
            "fn:count(fn:doc(\"hascontent\")//r)");
        assertTrue(result.hasNext());
        assertEquals("1500000", result.next().asString());
        Utils.closeSession();
    }
    
    //@Test
    public void testHTTPMultiChunksProxy() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();

        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                "xcc://admin:admin@ali:80"));
        
      try (Session session = cs.newSession()) {
        session.setTransactionMode(TransactionMode.UPDATE);
        Content content;
        byte[] str = getLargeString().getBytes();
        content = ContentFactory.newContent("hascontent", str, 0, str.length,
                options);
        session.insertContent(content);
        session.commit();
      }
        
        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@ali:80",
            "fn:count(fn:doc(\"hascontent\")//r)");
        assertTrue(result.hasNext());
        assertEquals("1500000", result.next().asString());
        Utils.closeSession();
    }

    private String getLargeString() {
        StringBuilder sb = new StringBuilder();
        sb.append("<root>");
        for (int i =0; i<1500000; i++) {
            sb.append("<r>some content</r>");
        }
        sb.append("</root>");
        return sb.toString();
    }
    
    //@Test
    public void testHTTPChunkingEncodingResolve() throws Exception {
        System.setProperty("xcc.httpcompliant", "true");
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();
        options.setResolveEntities(true);
        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                Utils.getDbXccUri()));
      try (Session session = cs.newSession()) {
        session.setTransactionMode(TransactionMode.UPDATE);
        Content content;/* = ContentFactory.newContent("nocontent", new byte[0],
        0, 0, options);
        session.insertContent(content);*/
        byte[] str = "<r>some content</r>".getBytes();
        content = ContentFactory.newContent("hascontent", str, 0, str.length,
                options);
        session.insertContent(content);
        session.commit();
      }
    }
    
    //@Test
    public void testXDBCChunkingEncoding() throws Exception {
//        Utils.clearDB(Utils.getDocumentsDbXccUri(), "Documents");
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();

        ContentSource cs = ContentSourceFactory.newContentSource(new URI(
                Utils.getDbXccUri()));
      try (Session session = cs.newSession()) {
        session.setTransactionMode(TransactionMode.UPDATE);
        Content content = ContentFactory.newContent("nocontent", new byte[0],
                0, 0, options);
        session.insertContent(content);
        byte[] str = "<r>some content</r>".getBytes();
        content = ContentFactory.newContent("hascontent", str, 0, str.length,
                options);
        session.insertContent(content);
        session.commit();
      }
    }
    

}