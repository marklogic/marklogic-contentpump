package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.ResultSequence;

public class TestImportArchive {
    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testBug19403() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/archive"
            + " -input_file_type archive"
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
    }
    
    @Test
    public void testArchiveWithNaked() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/mixnakedzip"
            + " -input_file_type archive"
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
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), 
            "fn:count(" +
            "for $i in cts:search(xdmp:document-properties(),cts:not-query(cts:document-fragment-query(cts:and-query( () )))) \n" +
            "where fn:contains(fn:base-uri($i),\"naked\") eq fn:false() \n" +
            "return fn:base-uri($i)" +
            ")");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
    }
    
    @Test
    public void testArchiveTransformWithNaked() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/mixnakedzip -fastload"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /lc_test.xqy"
            + " -input_file_type archive"
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
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), 
            "fn:count(" +
            "for $i in cts:search(xdmp:document-properties(),cts:not-query(cts:document-fragment-query(cts:and-query( () )))) \n" +
            "where fn:contains(fn:base-uri($i),\"naked\") eq fn:false() \n" +
            "return fn:base-uri($i)" +
            ")");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }
    
    @Test
    public void testArchiveTransformWithNakedTxn1() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/lc_test.xqy");
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/mixnakedzip -fastload -transaction_size 1"
            + " -transform_namespace http://marklogic.com/module_invoke"
            + " -transform_module /lc_test.xqy"
            + " -input_file_type archive"
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
        
        result = Utils.runQuery(
            Utils.getTestDbXccUri(), 
            "fn:count(" +
            "for $i in cts:search(xdmp:document-properties(),cts:not-query(cts:document-fragment-query(cts:and-query( () )))) \n" +
            "where fn:contains(fn:base-uri($i),\"naked\") eq fn:false() \n" +
            "return fn:base-uri($i)" +
            ")");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }
    
    @Test
    public void testBug19403_1() throws Exception {
        String cmd = "IMPORT -host localhost -username admin -password"
            + " admin -input_file_path " + Constants.TEST_PATH.toUri()
            + "/archive/wiki-000001.zip"
            + " -input_file_type archive"
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
        assertEquals("0", result.next().asString());
        Utils.closeSession();
    }
    
    @Test
    public void testBug38160() throws Exception {
        Utils.prepareModule(Utils.getTestDbXccUri(), "/38160/dummy-trans.xqy");
        String cmd = "IMPORT -host localhost -username admin -password admin "
                + "-input_file_path " + Constants.TEST_PATH.toUri() 
                + "/38160/20130327183855-0700-000000-XML.zip" + " -input_file_type archive "
                + "-transform_module /38160/dummy-trans.xqy "
                + "-transform_namespace my.dummy.transform.module "
                + "-output_permissions "
                + "admin,read,admin-builtins,read,admin-module-internal,read,"
                + "admin,insert,admin-builtins,insert,admin-module-internal,insert,"
                + "admin,update,admin-builtins,update,admin-module-internal,update,"
                + "admin,execute,admin-builtins,execute,admin-module-internal,execute"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" ");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        String permQry = "declare namespace sec = 'http://marklogic.com/xdmp/security';\n" + 
                "let $perms := xdmp:document-get-permissions('/bug18908/xml/1.xml')\n" + 
                "return (fn:count($perms/sec:capability[text()='read']), "
                + "fn:count($perms/sec:capability[text()='insert']), "
                + "fn:count($perms/sec:capability[text()='insert']), "
                + "fn:count($perms/sec:capability[text()='execute']))";
        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), permQry);
        
        for (int i = 0; i < 4; i++) {
            assertTrue(result.hasNext());
            assertEquals("3", result.next().asString());
        }
    }
}
