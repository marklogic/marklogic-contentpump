/**
 * 
 */
package com.marklogic.contentpump;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URISyntaxException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * @author mattsun
 *
 */
public class TestReturnCode {

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        Utils.closeSession();
    }

    @Test
    public void testReturnSuccess() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
                + " -output_uri_prefix test/"
                + " -output_collections test,ML"
                + " -fastload false"
                + " -output_uri_replace wiki,'wiki1'"
                + " -port " + Constants.port + " -database Documents";
        
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        int rc = ContentPump.runCommand(expandedArgs);
        assertTrue(rc==0);
    }
    
    @Test
    public void testReturnFail1() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki-nonexist"
                + " -output_uri_prefix test/"
                + " -output_collections test,ML"
                + " -fastload false"
                + " -output_uri_replace wiki,'wiki1'"
                + " -port " + Constants.port + " -database Documents";
        
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        int rc = ContentPump.runCommand(expandedArgs);
        assertTrue(rc==1);
    }

    @Test
    public void testReturnFail2() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        // import
        String cmd = 
            "IMPORT -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.TEST_PATH.toUri() + "/csv2.zip"
            + " -delimited_uri_id first"
            + " -input_compressed -input_compression_codec zip"
            + " -input_file_type delimited_text"
            + " -port " + Constants.port + " -database Documents";
        
        String[] args = cmd.split(" ");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        //export
        cmd = "EXPORT -host localhost -username admin -password admin"
            + " -output_file_path " + Constants.OUT_PATH.toUri()
            + " -output_type archive"
            + " -port " + Constants.port + " -database Documents";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        assertTrue(ContentPump.runCommand(expandedArgs)==0);
        
        cmd = "EXPORT -host localhost -username admin -password admin"
                + " -output_file_path " + Constants.OUT_PATH.toUri()
                + " -output_type archive"
                + " -port " + Constants.port + " -database Documents";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        int rc = ContentPump.runCommand(expandedArgs);
        System.out.print(rc);
        assertTrue(rc==1);
    }
    
    @Test
    public void testReturnFail3() throws Exception {
        String cmd = "IMPORT -password admin -username admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki"
                + " -output_uri_prefix test/"
                + " -output_collections test,ML"
                + " -fastload false"
                + " -output_uri_replace wiki,'wiki1'"
                + " -port " + Constants.port + " -database Documents";
        
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        int rc = ContentPump.runCommand(expandedArgs);
        assertTrue(rc==1);
    }
}
