/**
 * 
 */
package com.marklogic.contentpump;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.ResultSequence;

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
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        try {
          ContentPump.runCommand(expandedArgs);
        } catch (Exception e) {
          assertEquals(e.getMessage(),"host fake.host.com is not resolvable"); 
        }
        Utils.closeSession();
    }

    @Test
    public void testEDNonfastRestrict() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database " + Constants.testDb 
                + " -restrict_hosts";
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(),
                "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }

    @Test
    public void testEDFastNonrestrict2() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost,fake.host.com"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database " + Constants.testDb 
                + " -fastload";
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        try {
          ContentPump.runCommand(expandedArgs);
        } catch (Exception e) {
          assertEquals(e.getMessage(),"host fake.host.com is not resolvable"); 
        }
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }

    @Test
    public void testEDFastRestrict() throws Exception {
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database " + Constants.testDb 
                + " -fastload -restrict_hosts";
        String[] args = cmd.split(" +");
        assertFalse(args.length == 0);

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(),
                "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }

    @Test
    public void testEDExport() throws Exception {
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        // Prepare resources
        String cmd = "IMPORT -password admin -username admin -host localhost"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/wiki/AndorrA.xml"
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" +");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        Utils.deleteDirectory(new File(Constants.OUT_PATH.toUri().getPath()));
        // Export
        String cmdEx = "EXPORT -password admin -username admin -host localhost"
                + " -output_file_path " + Constants.OUT_PATH.toUri()
                + " -output_type archive"
                + " -port " + Constants.port + " -database " + Constants.testDb 
                + " -restrict_hosts";
        String[] argsEx = cmdEx.split(" +");
        assertFalse(argsEx.length == 0);

        String[] expandedArgsEx = null;
        expandedArgsEx = OptionsFileUtil.expandArguments(argsEx);
        ContentPump.runCommand(expandedArgsEx);
        // Import it back
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);

        cmd = "import -host localhost -username admin -password admin"
            + " -input_file_path " + Constants.OUT_PATH.toUri()
            + " -input_file_type archive"
            + " -port " + Constants.port + " -database " + Constants.testDb;
        args = cmd.split(" ");
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery(
            Utils.getTestDbXccUri(), "fn:count(fn:collection())");
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
                + " -port " + Constants.port + " -database " + Constants.testDb;
        String[] args = cmd.split(" +");

        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        ContentPump.runCommand(expandedArgs);
        // Copy
        String cmdEx = "COPY -input_password admin -input_username admin -input_port " + Constants.port 
                + " -output_password admin -output_username admin -output_port " + Constants.port
                + " -input_host localhost -output_host localhost"
                + " -input_database " + Constants.testDb + " -output_database " + Constants.copyDst
                + " -output_collections copycollection"
                + " -restrict_input_hosts -restrict_output_hosts";
        String[] argsEx = cmdEx.split(" +");
        assertFalse(argsEx.length == 0);

        String[] expandedArgsEx = null;
        expandedArgsEx = OptionsFileUtil.expandArguments(argsEx);
        ContentPump.runCommand(expandedArgsEx);
        
        ResultSequence result = Utils.runQuery(
            Utils.getCopyDbXccUri(), "fn:count(fn:collection('copycollection'))");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        Utils.closeSession();
    }
    
}
