/*
 * Copyright 2003-2019 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.contentpump;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.marklogic.mapreduce.MarkLogicConstants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.marklogic.contentpump.utilities.ReflectionUtil;

/**
 * Runs a job in-process, potentially multi-threaded.  Only supports map-only
 * jobs.
 * 
 * @author jchen
 *
 */
public class LocalJobRunner implements ConfigConstants {
    public static final Log LOG = LogFactory.getLog(LocalJobRunner.class);
    
    private LocalJob job;
    private ExecutorService pool;
    private AtomicInteger[] progress;
    private long startTime;
    private int threadsPerSplit = 0;
    private int threadCount;
    //TODO confusing, rename it
    private int availableThreads = 1;
    // minimally required thread per task defined by the job
    private int minThreads = 1;
    private int maxThreads;
    private Command cmd;
    private ContentPumpReporter reporter;
    
    public LocalJobRunner(LocalJob job, CommandLine cmdline, Command cmd) {
        this.job = job;
        this.cmd = cmd;

        if (cmdline.hasOption(THREAD_COUNT)) {
            threadCount = Integer.parseInt(
            		cmdline.getOptionValue(THREAD_COUNT));
        }
        
        if (cmdline.hasOption(THREADS_PER_SPLIT)) {
            threadsPerSplit = Integer.parseInt(
            		cmdline.getOptionValue(THREADS_PER_SPLIT));
        }

        if (cmdline.hasOption(MAX_THREADS)) {
            maxThreads = Integer.parseInt(
                cmdline.getOptionValue(MAX_THREADS));
        }

        Configuration conf = job.getConfiguration();
        minThreads = conf.getInt(CONF_MIN_THREADS, minThreads);

        startTime = System.currentTimeMillis();
    }

    /**
     * Run the job.  Get the input splits, create map tasks and submit it to
     * the thread pool if there is one; otherwise, runs the the task one by
     * one.
     * 
     * @param <INKEY>
     * @param <INVALUE>
     * @param <OUTKEY>
     * @param <OUTVALUE>
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public <INKEY,INVALUE,OUTKEY,OUTVALUE,
        T extends org.apache.hadoop.mapreduce.InputSplit> 
    void run() throws Exception {
        Configuration conf = job.getConfiguration();
        reporter = new ContentPumpReporter();
        InputFormat<INKEY,INVALUE> inputFormat = 
            (InputFormat<INKEY, INVALUE>)ReflectionUtils.newInstance(
                job.getInputFormatClass(), conf);
        List<InputSplit> splits;
        T[] array;
        try {
            splits = inputFormat.getSplits(job);
            array = (T[])splits.toArray(
                    new org.apache.hadoop.mapreduce.InputSplit[splits.size()]);
            // sort the splits into order based on size, so that the biggest
            // goes first
            Arrays.sort(array, new SplitLengthComparator());
        } catch (Exception ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error getting input splits: ", ex);
            } else {
                LOG.error("Error getting input splits: ");
                LOG.error(ex.getMessage());
            }
            job.setJobState(JobStatus.State.FAILED);
            return;
        }               
        OutputFormat<OUTKEY, OUTVALUE> outputFormat = 
            (OutputFormat<OUTKEY, OUTVALUE>)ReflectionUtils.newInstance(
                job.getOutputFormatClass(), conf);
        Class<? extends Mapper<?,?,?,?>> mapperClass = job.getMapperClass();
        Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper = 
            (Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)ReflectionUtils.newInstance(
                mapperClass, conf);
        try {
            outputFormat.checkOutputSpecs(job);
        } catch (Exception ex) {         
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error checking output specification: ", ex);
            } else {
                LOG.error("Error checking output specification: ");
                LOG.error(ex.getMessage());
            }
            job.setJobState(JobStatus.State.FAILED);
            return;
        }

        createThreadPool(conf);

        progress = new AtomicInteger[splits.size()];
        for (int i = 0; i < splits.size(); i++) {
            progress[i] = new AtomicInteger();
        }
     
        job.setJobState(JobStatus.State.RUNNING);
        Monitor monitor = new Monitor();
        monitor.start();
        List<Future<Object>> taskList = new ArrayList<Future<Object>>();
        for (int i = 0; i < array.length && !ContentPump.shutdown; i++) {        
            InputSplit split = array[i];
            if (pool != null) {
                LocalMapTask<INKEY, INVALUE, OUTKEY, OUTVALUE> task = 
                    new LocalMapTask<INKEY, INVALUE, OUTKEY, OUTVALUE>(
                        inputFormat, outputFormat, conf, i, split, reporter,
                        progress[i]);
                availableThreads = assignThreads(i, array.length);
                Class<? extends Mapper<?, ?, ?, ?>> runtimeMapperClass = 
                    job.getMapperClass();
                if (availableThreads > 1 && 
                    availableThreads != threadsPerSplit) { 
                	// possible runtime adjustment
                    if (runtimeMapperClass != (Class)MultithreadedMapper.class) {
                	    runtimeMapperClass = (Class<? extends 
                	        Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>>)
                	        cmd.getRuntimeMapperClass(job, mapperClass, 
                		        threadsPerSplit, availableThreads);
                    }   
                    if (runtimeMapperClass != mapperClass) {
                	    task.setMapperClass(runtimeMapperClass);
                    }
                    if (runtimeMapperClass == (Class)MultithreadedMapper.class) {
                	    task.setThreadCount(availableThreads);
                	    if (LOG.isDebugEnabled()) {
                            LOG.debug("Thread Count for Split#" + i + " : "
                                    + availableThreads);
                        }
                    }
                }
                
                if (runtimeMapperClass == (Class)MultithreadedMapper.class) {
                    synchronized (pool) {
                        taskList.add(pool.submit(task));
                        pool.wait();
                    }
                } else {
                	pool.submit(task);
                }
            } else { // single-threaded
                JobID jid = new JobID();
                TaskID taskId = new TaskID(jid.getJtIdentifier(), jid.getId(), TaskType.MAP, i);
                TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);
                TaskAttemptContext context = 
                    ReflectionUtil.createTaskAttemptContext(conf, taskAttemptId);
                RecordReader<INKEY, INVALUE> reader = 
                    inputFormat.createRecordReader(split, context);
                RecordWriter<OUTKEY, OUTVALUE> writer = 
                    outputFormat.getRecordWriter(context);
                OutputCommitter committer = 
                    outputFormat.getOutputCommitter(context);
                TrackingRecordReader trackingReader = 
                    new TrackingRecordReader(reader, progress[i]);

                Mapper.Context mapperContext = 
                    ReflectionUtil.createMapperContext(mapper, conf, 
                        taskAttemptId, trackingReader, writer, committer, 
                        reporter, split);
                
                trackingReader.initialize(split, mapperContext);
                
                // no thread pool (only 1 thread specified)
                Class<? extends Mapper<?,?,?,?>> mapClass = 
                        job.getMapperClass();
                mapperContext.getConfiguration().setClass(
                   CONF_MAPREDUCE_JOB_MAP_CLASS , mapClass, Mapper.class);
                mapper = (Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>) 
                    ReflectionUtils.newInstance(mapClass,
                        mapperContext.getConfiguration());
                try {
                    mapper.run(mapperContext);
                } finally {
                    try {
                        trackingReader.close();
                    } catch (Throwable t) {
                        LOG.error("Error closing reader: " + t.getMessage());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(t);
                        }
                    }
                    try {
                        writer.close(mapperContext);
                    } catch (Throwable t) {
                        LOG.error("Error closing writer: " + t.getMessage());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(t);
                        }
                    } 
                    try {
                        committer.commitTask(context);
                    } catch (Throwable t) {
                        LOG.error("Error committing task: " + t.getMessage());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(t);
                        }
                    }
                }
            }
        }
        // wait till all tasks are done
        if (pool != null) {
            pool.shutdown();
            while (!pool.awaitTermination(1, TimeUnit.HOURS));
        } 
        job.setJobState(JobStatus.State.SUCCEEDED);
        monitor.interrupt();
        monitor.join(1000);
        
        // report counters
        Iterator<CounterGroup> groupIt = 
            reporter.counters.iterator();
        while (groupIt.hasNext()) {
            CounterGroup group = groupIt.next();
            LOG.info(group.getDisplayName() + ": ");
            Iterator<Counter> counterIt = group.iterator();
            while (counterIt.hasNext()) {
                Counter counter = counterIt.next();
                LOG.info(counter.getDisplayName() + ": " + 
                                counter.getValue());
            }
        }
        LOG.info("Total execution time: " + 
                 (System.currentTimeMillis() - startTime) / 1000 + " sec");
    }
    
    /**
     * Assign thread count for a given split
     * 
     * @param splitIndex split index
     * @param splitCount
     * @return
     */
    private int assignThreads(int splitIndex, int splitCount) {
    	if (threadsPerSplit > 0) {
    		return threadsPerSplit;
    	}
        if (splitCount == 1) {
            return threadCount;
        }
        if (splitCount * minThreads > threadCount) {
            return minThreads;
        }
        if (splitIndex % threadCount < threadCount % splitCount) {
            return threadCount / splitCount + 1;
        } else {
            return threadCount / splitCount;
        }
    }

    private void createThreadPool(Configuration conf) {
        int numThreads;
        if (threadCount != 0) {
            // Use specified threadCount in the command line
            numThreads = threadCount;
        } else {
            // Use server max thread count
            numThreads =  conf.getInt(
                MarkLogicConstants.SERVER_THREAD_COUNT, DEFAULT_THREAD_COUNT);
            if (numThreads == 0) {
                // ML server version is below 10.0-4.2, unable to get server
                // thread count
                numThreads = DEFAULT_THREAD_COUNT;
            } else {
                numThreads =
                    (int)(MarkLogicConstants.THREAD_MULTIPLIER * numThreads);
            }
        }
        threadCount = Math.max(numThreads, minThreads);
        if (maxThreads > 0) {
            threadCount = Math.min(threadCount, maxThreads);
        }
        if (threadCount > 1) {
            pool = Executors.newFixedThreadPool(threadCount);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Thread pool size: " + threadCount);
            }
        }
    }
    
    /**
     * A map task to be run in a thread.
     * 
     * @author jchen
     *
     * @param <INKEY>
     * @param <INVALUE>
     * @param <OUTKEY>
     * @param <OUTVALUE>
     */
    public class LocalMapTask<INKEY,INVALUE,OUTKEY,OUTVALUE>
    implements Callable<Object> {
        private InputFormat<INKEY, INVALUE> inputFormat;
        private OutputFormat<OUTKEY, OUTVALUE> outputFormat;
        private Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper;
        private Configuration conf;
        private int id;
        private InputSplit split;
        private AtomicInteger pctProgress;
        private ContentPumpReporter reporter;
        private Class<? extends Mapper<?,?,?,?>> mapperClass;
        private int threadCount = 0;
        
        public LocalMapTask(InputFormat<INKEY, INVALUE> inputFormat, 
                OutputFormat<OUTKEY, OUTVALUE> outputFormat, 
                Configuration conf, int id, InputSplit split, 
                ContentPumpReporter reporter, AtomicInteger pctProgress) {
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.conf = conf;
            this.id = id;
            this.split = split;
            this.pctProgress = pctProgress;
            this.reporter = reporter;
            try {
				mapperClass = job.getMapperClass();
			} catch (ClassNotFoundException e) {
				LOG.error("Mapper class not found", e);
			}
        }
        
        public int getThreadCount() {
        	return threadCount;
        }
        
        public void setThreadCount(int threads) {
			threadCount = threads;		
		}

		public Class<? extends Mapper<?,?,?,?>> getMapperClass() {
        	return mapperClass;
        }
        
		public void setMapperClass(
				Class<? extends Mapper<?,?,?,?>> runtimeMapperClass) {
			mapperClass =  runtimeMapperClass;		
		}

		@SuppressWarnings("unchecked")
        @Override
        public Object call() {
            TaskAttemptContext context = null;
            Mapper.Context mapperContext = null;
            TrackingRecordReader trackingReader = null;
            RecordWriter<OUTKEY, OUTVALUE> writer = null;
            OutputCommitter committer = null;
            JobID jid = new JobID();
            TaskID taskId = new TaskID(jid.getJtIdentifier(), jid.getId(), TaskType.MAP, id);
            TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);
            try {
                context = ReflectionUtil.createTaskAttemptContext(conf, 
                        taskAttemptId);
                RecordReader<INKEY, INVALUE> reader = 
                    inputFormat.createRecordReader(split, context);
                writer = outputFormat.getRecordWriter(context);
                committer = outputFormat.getOutputCommitter(context);
                trackingReader = 
                    new TrackingRecordReader(reader, pctProgress);
                mapper = (Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
                  ReflectionUtils.newInstance(mapperClass, conf);
                mapperContext = ReflectionUtil.createMapperContext(mapper, 
                        conf, taskAttemptId, trackingReader, writer, committer,
                        reporter, split);
                trackingReader.initialize(split, mapperContext);
                if (mapperClass == (Class)MultithreadedMapper.class) {
                	((MultithreadedMapper)mapper).setThreadCount(threadCount);
                    ((MultithreadedMapper)mapper).setThreadPool(pool);
                }
                mapper.run(mapperContext);
            } catch (Throwable t) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Error running task: ", t);
                } else {
                    LOG.error("Error running task: ");
                    LOG.error(t.getMessage());
                }
                try {
                    synchronized(pool) {
                        pool.notify();
                    }
                } catch (Throwable t1) {
                    LOG.error(t1);
                }
            }
            try {
                if (trackingReader != null) {
                    trackingReader.close();
                }
            } catch (Throwable t) {
                LOG.error("Error closing reader: " + t.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(t);
                }
            } 
            try {
                if (writer != null) {
                    writer.close(mapperContext);
                }
            } catch (Throwable t) {
                LOG.error("Error closing writer: " + t.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(t);
                }
            } 
            try {
                committer.commitTask(context);
            } catch (Throwable t) {
                LOG.error("Error committing task: " + t.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(t);
                }
            } 
            return null;
        }      
    }
    
    class TrackingRecordReader<K, V> extends RecordReader<K, V> {
        private final RecordReader<K, V> real;
        private AtomicInteger pctProgress;

        TrackingRecordReader(RecordReader<K, V> real, 
                        AtomicInteger pctProgress) {
            this.real = real;
            this.pctProgress = pctProgress;
        }

        @Override
        public void close() throws IOException {
            real.close();
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return real.getCurrentKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return real.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return real.getProgress();
        }

        @Override
        public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
                        org.apache.hadoop.mapreduce.TaskAttemptContext context)
                        throws IOException, InterruptedException {
            real.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean result = real.nextKeyValue();
            pctProgress.set((int) (getProgress() * 100));
            return result;
        }
    }
    
    class Monitor extends Thread {
        private String lastReport;
        
        public void run() {
            try {
                while (!ContentPump.shutdown && !interrupted() &&
                        !job.done()) {
                    Thread.sleep(1000);
                    if (ContentPump.shutdown) {
                        break;
                    }
                    String report = 
                        (" completed " + 
                            StringUtils.formatPercent(computeProgress(), 0));
                    if (!report.equals(lastReport)) {
                        LOG.info(report);
                        lastReport = report;
                    }
                }
            } catch (InterruptedException e) {
            } catch (Throwable t) {
                LOG.error("Error in monitor thread", t);
            }
            String report = 
                (" completed " + 
                    StringUtils.formatPercent(computeProgress(), 0));
            if (!report.equals(lastReport)) {
                LOG.info(report);
            }
        }
    }

    public double computeProgress() {
        if (progress.length == 0) {
            return (double)1;
        }
        long result = 0;
        for (AtomicInteger pct : progress) {
            result += pct.longValue();
        }
        return (double)result / progress.length / 100;
    }
    
    private static class SplitLengthComparator implements
            Comparator<org.apache.hadoop.mapreduce.InputSplit> {

        @Override
        public int compare(org.apache.hadoop.mapreduce.InputSplit o1,
                org.apache.hadoop.mapreduce.InputSplit o2) {
            try {
                long len1 = o1.getLength();
                long len2 = o2.getLength();
                if (len1 < len2) {
                    return 1;
                } else if (len1 == len2) {
                    return 0;
                } else {
                    return -1;
                }
            } catch (IOException ie) {
                throw new RuntimeException("exception in compare", ie);
            } catch (InterruptedException ie) {
                throw new RuntimeException("exception in compare", ie);
            }
        }
    }

    public ContentPumpReporter getReporter() {
        return this.reporter;
    }
}
