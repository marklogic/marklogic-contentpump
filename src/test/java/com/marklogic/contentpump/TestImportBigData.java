/**
 * 
 */
package com.marklogic.contentpump;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.SimpleTimeLimiter;

/**
 * @author mattsun
 *
 */
public class TestImportBigData {

    /**
     * 
     */
    public TestImportBigData() {
        // TODO Auto-generated constructor stub
    }
    
    @After
    public void tearDown() {
        Utils.closeSession();
    }

    /**
     * This test measures the performance of FileAndDirectoryInputFormat.getSplits
     * If it takes more than two minutes to execute getSplits, than an exception will throw
     * @throws Exception
     */
    @Test
    public void testBug35949() throws Exception{
        Callable<Void> callGetSplit = new Callable<Void>() {
            public Void call() throws Exception{
                Configuration conf = new Configuration();
                conf.set("mapreduce.input.fileinputformat.inputdir", 
                        Constants.TEST_PATH.toUri() + "/35949");
                Job job = Job.getInstance(conf);
                job.setInputFormatClass(CombineDocumentInputFormat.class);
                
                FileInputFormat<?, ?> inputFormat = new CombineDocumentInputFormat();
                List<InputSplit> splits = inputFormat.getSplits(job);
                
                return null;
            }
        };
        String pathWithoutSchema = Path.getPathWithoutSchemeAndAuthority(Constants.TEST_PATH).toString();
        Utils.unzip(pathWithoutSchema + "/35949.zip", pathWithoutSchema);
        
        SimpleTimeLimiter stl = SimpleTimeLimiter.create(Executors.newCachedThreadPool());     
        
        String system = System.getProperty("os.name");
        if (!system.contains("win")) {
            Stopwatch sw = Stopwatch.createStarted();
            stl.callWithTimeout(callGetSplit, 3L, TimeUnit.MINUTES);
            sw.stop();
            long time = sw.elapsed(TimeUnit.SECONDS);
            System.out.println("Total time of listStatus: " + time + " seconds.");
            assertTrue(time < 25L);
        }        
        
        
        File dir = new File(pathWithoutSchema + "/35949");
        FileUtils.deleteDirectory(dir);
    }
}
