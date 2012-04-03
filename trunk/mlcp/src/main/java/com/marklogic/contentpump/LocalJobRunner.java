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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Runs a job in-process, potentially multi-threaded.  Only supports map-only
 * jobs.
 * 
 * @author jchen
 *
 */
public class LocalJobRunner implements ConfigConstants {
    public static final Log LOG = LogFactory.getLog(LocalJobRunner.class);
    
    private Job job;
    private ExecutorService pool;
    
    public LocalJobRunner(Job job, CommandLine cmdline) {
        this.job = job;
        
        // Default thread count is 1.
        if (cmdline.hasOption(THREAD_COUNT)) {
            int threadCount = Integer.parseInt(
                    cmdline.getOptionValue(THREAD_COUNT));
            if (threadCount > 1) {
                pool = Executors.newFixedThreadPool(threadCount);
            }
        }
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
        outputFormat.checkOutputSpecs(job);
        conf = job.getConfiguration();
      
        for (int i = 0; i < splits.size(); i++) {        
            InputSplit split = splits.get(i);
            if (pool != null) {
                LocalMapTask<INKEY, INVALUE, OUTKEY, OUTVALUE> task = 
                    new LocalMapTask<INKEY, INVALUE, OUTKEY, OUTVALUE>(job,
                            inputFormat, outputFormat, conf, i, split);
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
                // TODO: find a way to leverage the StatusReporter
                Mapper.Context mapperContext = mapper.new Context(conf,
                        taskAttemptId, reader, writer, committer, 
                        (StatusReporter)null, split);
                
                reader.initialize(split, mapperContext);
                mapper.run(mapperContext);
                reader.close();
                writer.close(mapperContext);
                committer.commitTask(context);
            }
        }
        if (pool != null) {
            pool.shutdown();
        }
        // wait forever till all tasks are done
        while (!pool.awaitTermination(1, TimeUnit.DAYS));
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
    public static class LocalMapTask<INKEY,INVALUE,OUTKEY,OUTVALUE>
    implements Callable<Object> {
        private Job job;
        private InputFormat<INKEY, INVALUE> inputFormat;
        private OutputFormat<OUTKEY, OUTVALUE> outputFormat;
        private Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper;
        private Configuration conf;
        private int id;
        private InputSplit split;
        
        public LocalMapTask(Job job, InputFormat<INKEY, INVALUE> inputFormat, 
                OutputFormat<OUTKEY, OUTVALUE> outputFormat, 
                Configuration conf, int id, InputSplit split) {
            this.job = job;
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.conf = conf;
            this.id = id;
            this.split = split;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Object call() throws Exception {
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
            
            Mapper.Context mapperContext = mapper.new Context(conf,
                    taskAttemptId, reader, writer, committer, 
                    (StatusReporter)null, split);
            
            reader.initialize(split, mapperContext);
            mapper.run(mapperContext);
            reader.close();
            writer.close(mapperContext);
            committer.commitTask(context);

            return null;
        }      
    }
}
