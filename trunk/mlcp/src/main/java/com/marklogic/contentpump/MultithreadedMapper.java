/*
 * Copyright 2003-2013 MarkLogic Corporation
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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Multithreaded implementation for @link org.apache.hadoop.mapreduce.Mapper.
 * <p>
 * It can be used instead of the default implementation,
 * 
 * @link org.apache.hadoop.mapred.MapRunner, when the Map operation is neither
 *       CPU bound nor IO bound in order to improve throughput.
 *       <p>
 *       Mapper implementations using this MapRunnable must be thread-safe.
 *       <p>
 *       The Map-Reduce job can be configured with the mapper to use via
 *       {@link #setMapperClass(Configuration, Class)} and the number of thread
 *       the thread-pool can use with the
 *       {@link #getNumberOfThreads(Configuration) method. The default value is
 *       10 threads.
 *       <p>
 */
public class MultithreadedMapper<K1, V1, K2, V2> extends
    Mapper<K1, V1, K2, V2> {
    private static final Log LOG = LogFactory
        .getLog(MultithreadedMapper.class);
    private Class<? extends Mapper<K1, V1, K2, V2>> mapClass;
    private Context outer;
    private List<MapRunner> runners;

    /**
     * The number of threads in the thread pool that will run the map function.
     * 
     * @param job
     *            the job
     * @return the number of threads
     */
    public static int getNumberOfThreads(JobContext job) {
        return job.getConfiguration().getInt(
            ConfigConstants.CONF_THREADS_PER_SPLIT, 10);
    }

    /**
     * Set the number of threads in the pool for running maps.
     * 
     * @param job
     *            the job to modify
     * @param threads
     *            the new number of threads
     */
    public static void setNumberOfThreads(Job job, int threads) {

        job.getConfiguration().setInt(ConfigConstants.CONF_THREADS_PER_SPLIT,
            threads);
    }

    public static void setNumberOfThreads(Configuration conf, int threads) {

        conf.setInt(ConfigConstants.CONF_THREADS_PER_SPLIT, threads);

    }

    /**
     * Get the application's mapper class.
     * 
     * @param <K1>
     *            the map's input key type
     * @param <V1>
     *            the map's input value type
     * @param <K2>
     *            the map's output key type
     * @param <V2>
     *            the map's output value type
     * @param job
     *            the job
     * @return the mapper class to run
     */
    @SuppressWarnings("unchecked")
    public static <K1, V1, K2, V2> Class<Mapper<K1, V1, K2, V2>> getMapperClass(
        JobContext job) {
        return (Class<Mapper<K1, V1, K2, V2>>) job.getConfiguration()
            .getClass(ConfigConstants.CONF_MULTITHREADEDMAPPER_CLASS,
                Mapper.class);
    }

    /**
     * Set the application's mapper class.
     * 
     * @param <K1>
     *            the map input key type
     * @param <V1>
     *            the map input value type
     * @param <K2>
     *            the map output key type
     * @param <V2>
     *            the map output value type
     * @param job
     *            the job to modify
     * @param cls
     *            the class to use as the mapper
     */
    @SuppressWarnings("rawtypes")
    public static <K1, V1, K2, V2> void setMapperClass(Configuration conf,
        Class<? extends Mapper> cls) {
        if (MultithreadedMapper.class.isAssignableFrom(cls)) {
            throw new IllegalArgumentException("Can't have recursive "
                + "MultithreadedMapper instances.");
        }
        conf.setClass(ConfigConstants.CONF_MULTITHREADEDMAPPER_CLASS, cls,
            Mapper.class);
    }

    @SuppressWarnings("unchecked")
    public void run(Context context, ExecutorService pool) throws IOException,
        InterruptedException, ExecutionException, ClassNotFoundException {
        outer = context;
        int numberOfThreads = getNumberOfThreads(context);
        mapClass = getMapperClass(context);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Configuring multithread mapper to use "
                + numberOfThreads + " threads");
        }
        // current mapper takes 1 thread
        numberOfThreads--;

        runners = new ArrayList<MapRunner>(numberOfThreads);
        List<Future<Object>> taskList = new ArrayList<Future<Object>>();
        synchronized (pool) {
            for (int i = 0; i < numberOfThreads; ++i) {
                MapRunner thread;
                thread = new MapRunner(context);
                if (!pool.isShutdown()) {
                    taskList.add((Future<Object>) pool.submit(thread));
                } else {
                    throw new InterruptedException(
                        "Thread Pool has been shut down");
                }
            }
            pool.notify();
        }

        // MapRunner that runs in current thread
        MapRunner r = new MapRunner(context);
        r.run();

        for (Future<Object> f : taskList) {
            f.get();
        }
    }

    /**
     * Run the application's maps using a thread pool.
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        outer = context;
        int numberOfThreads = getNumberOfThreads(context);
        mapClass = getMapperClass(context);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Configuring multithread runner to use "
                + numberOfThreads + " threads");
        }
        // current mapper takes 1 thread
        numberOfThreads--;

        runners = new ArrayList<MapRunner>(numberOfThreads);
        try {
            for (int i = 0; i < numberOfThreads; ++i) {
                MapRunner thread;
                thread = new MapRunner(context);
                thread.start();
                runners.add(i, thread);
            }
            // MapRunner runs in current thread
            MapRunner r = new MapRunner(context);
            r.run();
        } catch (ClassNotFoundException e) {
            LOG.error(e);
        }

        for (int i = 0; i < numberOfThreads; ++i) {
            MapRunner thread = runners.get(i);
            thread.join();
            Throwable th = thread.throwable;
            if (th != null) {
                if (th instanceof IOException) {
                    throw (IOException) th;
                } else if (th instanceof InterruptedException) {
                    throw (InterruptedException) th;
                } else {
                    throw new RuntimeException(th);
                }
            }
        }
    }

    private class SubMapRecordReader extends RecordReader<K1, V1> {
        private K1 key;
        private V1 value;

        @Override
        public void close() throws IOException {
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            synchronized (outer) {
                if (!outer.nextKeyValue()) {
                    return false;
                }
                if (outer.getCurrentKey() == null) {
                    return true;
                }
                key = ReflectionUtils.copy(outer.getConfiguration(),
                    outer.getCurrentKey(), key);
                value = (V1) ReflectionUtils.newInstance(outer
                    .getCurrentValue().getClass(), outer.getConfiguration());
                value = ReflectionUtils.copy(outer.getConfiguration(),
                    outer.getCurrentValue(), value);
                return true;
            }
        }

        public K1 getCurrentKey() {
            return key;
        }

        @Override
        public V1 getCurrentValue() {
            return value;
        }
    }

    private class SubMapStatusReporter extends StatusReporter {

        @Override
        public Counter getCounter(Enum<?> name) {
            return outer.getCounter(name);
        }

        @Override
        public Counter getCounter(String group, String name) {
            return outer.getCounter(group, name);
        }

        @Override
        public void progress() {
            outer.progress();
        }

        @Override
        public void setStatus(String status) {
            outer.setStatus(status);
        }

    }

    private class MapRunner extends Thread {
        private Mapper<K1, V1, K2, V2> mapper;
        private Context subcontext;
        private Throwable throwable;
        private RecordWriter writer;

        @SuppressWarnings("rawtypes")
        MapRunner(Context context) throws IOException, InterruptedException,
            ClassNotFoundException {
            // initiate the real mapper (DocumentMapper) that does the work
            mapper = ReflectionUtils.newInstance(mapClass,
                context.getConfiguration());
            OutputFormat outputFormat = (OutputFormat) ReflectionUtils
                .newInstance(outer.getOutputFormatClass(),
                    outer.getConfiguration());
            writer = outputFormat.getRecordWriter(outer);
            subcontext = new Context(outer.getConfiguration(),
                outer.getTaskAttemptID(), new SubMapRecordReader(), writer,
                context.getOutputCommitter(), new SubMapStatusReporter(),
                outer.getInputSplit());
        }

        @Override
        public void run() {
            try {
                mapper.run(subcontext);
                writer.close(subcontext);
            } catch (Throwable ie) {
                LOG.error(ie.getMessage(), ie);
                throwable = ie;
            }
        }
    }

}
