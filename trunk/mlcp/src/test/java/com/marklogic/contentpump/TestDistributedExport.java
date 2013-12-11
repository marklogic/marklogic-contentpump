package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

public class TestDistributedExport {
    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    //can only work if client co-locate with hadoop server
//    @Test
//    public void testExportZip() throws Exception {
//        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
//        String cmd = 
//            "IMPORT -host localhost -port 5275 -username admin -password admin"
//            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
//            + " -delimited_uri_id first"
//            + " -input_compressed -input_compression_codec zip"
//            + " -input_file_type delimited_text"
//            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR;
//        
//        String[] args = cmd.split(" ");
//
//        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
//
//        String[] expandedArgs = null;
//        expandedArgs = OptionsFileUtil.expandArguments(args);
//        Utils.prepareDistributedMode();
//        ContentPump.runCommand(expandedArgs);
//
//        ResultSequence result = Utils.runQuery(
//            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
//        assertTrue(result.hasNext());
//        assertEquals("5", result.next().asString());
//        Utils.closeSession();
//        
//        //export to local filesystem in local mode
//        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
//            + " -output_file_path " + Constants.OUT_PATH.toUri()
//            + " -output_type document -compress";
//        args = cmd.split(" ");
//        expandedArgs = OptionsFileUtil.expandArguments(args);
//        ContentPump.runCommand(expandedArgs);
//        
//        //import it back
//        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
//
//        cmd = "import -host localhost -port 5275 -username admin -password admin"
//            + " -input_file_path " + Constants.OUT_PATH.toUri()
//            + " -input_file_type documents -document_type xml"
//            + " -input_compressed true"
//            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR;
//        args = cmd.split(" ");
//        expandedArgs = OptionsFileUtil.expandArguments(args);
//        ContentPump.runCommand(expandedArgs);
//        
//        result = Utils.runQuery(
//            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
//        assertTrue(result.hasNext());
//        assertEquals("5", result.next().asString());
//        Utils.closeSession();
//    }
    
    @Test
    public void testExportZipToHDFS() throws Exception {

        String cmd = 
            "IMPORT -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR;
        
        String[] args = cmd.split(" ");

        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);

        ResultSequence result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
        
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssZ");
        String timestamp = sdf.format(date);
        
        //export
        cmd = "EXPORT -host localhost -port 5275 -username admin -password admin"
            + " -output_file_path " + "tmp/" + timestamp + "/test"
            + " -output_type document -compress"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        //import it back
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");

        cmd = "import -host localhost -port 5275 -username admin -password admin"
            + " -input_file_path " + "tmp/" + timestamp
            + " -input_file_type documents -document_type xml"
            + " -input_compressed true"
            + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        result = Utils.runQuery(
            "xcc://admin:admin@localhost:5275", "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        Utils.closeSession();
    }
}
