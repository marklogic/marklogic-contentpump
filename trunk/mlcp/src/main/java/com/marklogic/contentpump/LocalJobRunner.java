/*
 * Copyright 2003-2012 MarkLogic Corporation
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Runs a job in-process, potentially multi-threaded.  Only supports map-only
 * jobs.
 * 
 * @author jchen
 *
 */
public class LocalJobRunner implements ConfigConstants {
    public static final Log LOG = LogFactory.getLog(LocalJobRunner.class);
    public static final int DEFAULT_IMPORT_THREAD_COUNT = 4;
    public static final int DEFAULT_COPY_THREAD_COUNT = 4;
    public static final int DEFAULT_EXPORT_THREAD_COUNT = 4;
    
    
    private Job job;
    private ExecutorService pool;
    private AtomicInteger[] progress;
    private AtomicBoolean jobComplete;
    private long startTime;
    
    public LocalJobRunner(Job job, CommandLine cmdline, Command cmd) {
        this.job = job;
        int threadCount = 1;
        switch (cmd) {
        case IMPORT:
            threadCount = DEFAULT_IMPORT_THREAD_COUNT;
            break;
        case COPY:
            threadCount = DEFAULT_COPY_THREAD_COUNT;
            break;
        case EXPORT:
            threadCount = DEFAULT_EXPORT_THREAD_COUNT;
            break;
        }
        if (cmdline.hasOption(THREAD_COUNT)) {
            threadCount = Integer.parseInt(cmdline
                .getOptionValue(THREAD_COUNT));
        }
        if (threadCount > 1) {
            pool = Executors.newFixedThreadPool(threadCount);
        }
        jobComplete = new AtomicBoolean();
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
    public <INKEY,INVALUE,OUTKEY,OUTVALUE>
    void run() throws Exception {
        Configuration conf = job.getConfiguration();
        InputFormat<INKEY,INVALUE> inputFormat = 
            (InputFormat<INKEY, INVALUE>)ReflectionUtils.newInstance(
                job.getInputFormatClass(), conf);
        List<InputSplit> splits = inputFormat.getSplits(job);
        OutputFormat<OUTKEY, OUTVALUE> outputFormat = 
            (OutputFormat<OUTKEY, OUTVALUE>)ReflectionUtils.newInstance(
                job.getOutputFormatClass(), conf);
        Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper = 
            (Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)ReflectionUtils.newInstance(
                job.getMapperClass(), conf);
        try {
            outputFormat.checkOutputSpecs(job);
        } catch (Exception ex) {         
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error checking output specification: ", ex);
            } else {
                LOG.error("Error checking output specification: ");
                LOG.error(ex.getMessage());
            }
            return;
        }
        conf = job.getConfiguration();
        progress = new AtomicInteger[splits.size()];
        for (int i = 0; i < splits.size(); i++) {
            progress[i] = new AtomicInteger();
        }
        Monitor monitor = new Monitor();
        monitor.start();
        ContentPumpReporter reporter = new ContentPumpReporter();
        
        for (int i = 0; i < splits.size(); i++) {        
            InputSplit split = splits.get(i);
            if (pool != null) {
                LocalMapTask<INKEY, INVALUE, OUTKEY, OUTVALUE> task = 
                    new LocalMapTask<INKEY, INVALUE, OUTKEY, OUTVALUE>(job,
                        inputFormat, outputFormat, conf, i, split, reporter,
                        progress[i]);
                pool.submit(task);
            } else {
                TaskID taskId = new TaskID(new JobID(), true, i);
                TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);
                TaskAttemptContext context = new TaskAttemptContext(conf, 
                        taskAttemptId);
                RecordReader<INKEY, INVALUE> reader = 
                    inputFormat.createRecordReader(split, context);
                RecordWriter<OUTKEY, OUTVALUE> writer = 
                    outputFormat.getRecordWriter(context);
                OutputCommitter committer = 
                    outputFormat.getOutputCommitter(context);
                TrackingRecordReader trackingReader = 
                    new TrackingRecordReader(reader, progress[i]);
                
                Mapper.Context mapperContext = mapper.new Context(conf,
                        taskAttemptId, trackingReader, writer, committer, 
                        reporter, split);
                
                trackingReader.initialize(split, mapperContext);
                mapper.run(mapperContext);
                trackingReader.close();
                writer.close(mapperContext);
                committer.commitTask(context);
            }
        }
        if (pool != null) {
            pool.shutdown();
            // wait forever till all tasks are done
            while (!pool.awaitTermination(1, TimeUnit.DAYS));
            jobComplete.set(true);
        }
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
        private Job job;
        private InputFormat<INKEY, INVALUE> inputFormat;
        private OutputFormat<OUTKEY, OUTVALUE> outputFormat;
        private Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper;
        private Configuration conf;
        private int id;
        private InputSplit split;
        private AtomicInteger pctProgress;
        private ContentPumpReporter reporter;
        
        public LocalMapTask(Job job, InputFormat<INKEY, INVALUE> inputFormat, 
                OutputFormat<OUTKEY, OUTVALUE> outputFormat, 
                Configuration conf, int id, InputSplit split, 
                ContentPumpReporter reporter, AtomicInteger pctProgress) {
            this.job = job;
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.conf = conf;
            this.id = id;
            this.split = split;
            this.pctProgress = pctProgress;
            this.reporter = reporter;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Object call() {
            Mapper.Context mapperContext = null;
            try {
                TaskID taskId = new TaskID(new JobID(), true, id);
                TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);
                TaskAttemptContext context = new TaskAttemptContext(conf, 
                        taskAttemptId);
                RecordReader<INKEY, INVALUE> reader = 
                    inputFormat.createRecordReader(split, context);
                RecordWriter<OUTKEY, OUTVALUE> writer = 
                    outputFormat.getRecordWriter(context);
                OutputCommitter committer = 
                    outputFormat.getOutputCommitter(context);
                mapper = 
                    (Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
                    ReflectionUtils.newInstance(job.getMapperClass(), conf);
                TrackingRecordReader trackingReader = 
                    new TrackingRecordReader(reader, pctProgress);
                mapperContext = mapper.new Context(conf,
                        taskAttemptId, trackingReader, writer, committer, 
                        reporter, split);
                
                trackingReader.initialize(split, mapperContext);
                mapper.run(mapperContext);
                trackingReader.close();
                writer.close(mapperContext);
                committer.commitTask(context);                
            } catch (Throwable t) {
                LOG.error("Error running task: " + 
                                mapperContext.getTaskAttemptID(), t);
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
                while (!jobComplete.get() && !interrupted()) {
                    Thread.sleep(1000);
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
        long result = 0;
        for (AtomicInteger pct : progress) {
            result += pct.longValue();
        }
        return (double)result / progress.length / 100;
    }
}
