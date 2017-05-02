package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.ResultSequence;

public class TestConcurrentJobs {
    @Test
    public void testConcurrentJobs() throws Exception {
        String cmd = 
                "IMPORT -password admin -username admin -host localhost"
                + " -thread_count 1"
                + " -port " + Constants.port + " -database " 
                + Constants.testDb 
                + " -input_file_path " + Constants.TEST_PATH.toUri() 
                + "/wiki";
        String[] args = cmd.split(" +");
        final String[] expandedArgs = OptionsFileUtil.expandArguments(args);
        
        Utils.clearDB(Utils.getTestDbXccUri(), Constants.testDb);
        
        Thread job = new Thread() {
            public void run() {
                try {
                    ContentPump.runCommand(expandedArgs);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }; 
        job.start();
        Thread.sleep(1000);
        
        ContentPump.runCommand(expandedArgs);
        job.join(0);

        ResultSequence result = Utils.runQuery(
                Utils.getTestDbXccUri(), "fn:count(fn:collection())");
        assertTrue(result.hasNext());
        assertEquals("93", result.next().asString());
        Utils.closeSession();
        AssignmentManager.getInstance().setInitialized(false);
    }
}
