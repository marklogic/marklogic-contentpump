/**
 * @author mattsun
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
public class TestEDSeparation {

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
    public void testEDNonfastNonrestrict() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost,fake.host.com"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database Documents";
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
                Utils.getDbXccUri(),
                "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testEDNonfastRestrict() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database Documents"
                + " -restrict_hosts";
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
                Utils.getDbXccUri(),
                "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testEDFastNonrestrict2() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost,fake.host.com"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database Documents"
                + " -fastload";
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
                Utils.getDbXccUri(),
                "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testEDFastRestrict() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database Documents"
                + " -fastload -restrict_hosts";
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
                Utils.getDbXccUri(),
                "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testEDExport() throws Exception {
        // Prepare resources
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database Documents";
        String[] args = cmd.split(" +");

        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        // Export
        String cmdEx = "EXPORT -password admin -username admin -host localhost"
                + " -output_file_path " + Constants.OUT_PATH.toUri()
                + " -output_type archive"
                + " -port " + Constants.port + " -database Documents"
                + " -restrict_hosts";
        String[] argsEx = cmdEx.split(" +");
        assertFalse(argsEx.length == 0);

        String[] expandedArgsEx = null;
        expandedArgsEx = OptionsFileUtil.expandArguments(argsEx);
        ContentPump.runCommand(expandedArgsEx);
        // Import it back
        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);

        cmd = "import -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.OUT_PATH.toUri()
            + " -input_file_type archive"
            + " -port " + Constants.port + " -database Documents";
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
            Utils.getDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
    }

    @Test
    public void testEDCopy() throws Exception {
        // Prepare resources
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database Documents";
        String[] args = cmd.split(" +");

        Utils.clearDB(Utils.getDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        // Export
        String cmdEx = "COPY -input_password admin -input_username admin -input_port " + Constants.port 
                + " -output_password admin -output_username admin -output_port " + Constants.port
                + " -input_host localhost -output_host localhost"
                + " -input_database Documents -output_database Documents"
                + " -output_collections copycollection"
                + " -restrict_input_hosts -restrict_output_hosts";
        String[] argsEx = cmdEx.split(" +");
        assertFalse(argsEx.length == 0);

        String[] expandedArgsEx = null;
        expandedArgsEx = OptionsFileUtil.expandArguments(argsEx);
        ContentPump.runCommand(expandedArgsEx);
        
        ResultSequence result = Utils.runQuery(
            Utils.getDbXccUri(), "fn:count(fn:collection('copycollection'))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
}
