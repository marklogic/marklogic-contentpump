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
import java.lang.reflect.Method;
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

import com.marklogic.contentpump.utilities.ReflectionUtil;

/**
 * Multithreaded implementation for @link org.apache.hadoop.mapreduce.Mapper,
 * currently used by import operations when applicable to leverage concurrent
 * updates to MarkLogic.
 */
public class MultithreadedMapper<K1, V1, K2, V2> extends
    Mapper<K1, V1, K2, V2> {
    private static final Log LOG = LogFactory
        .getLog(MultithreadedMapper.class);
    private Class<? extends Mapper<K1, V1, K2, V2>> mapClass;
    private Context outer;
    private List<MapRunner> runners;
    private int threadCount = 0;
    private ExecutorService threadPool;

    /**
     * Get thread count set for this mapper.
     * @return thread count set for this mapper.
     */
    public int getThreadCount(Context context) {
		if (threadCount > 0) {
			return threadCount;
		} else {
			return getNumberOfThreads(context);
		}
	}

    /**
     * Set thread count for this mapper.
     * @param threadCount Thread count for this mapper.
     */
	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}

	/**
	 * Get a thread pool to be used for this mapper.
	 * @return thread pool to be used for this mapper.
	 */
	public ExecutorService getThreadPool() {
		return threadPool;
	}

	/**
	 * Set the thread pool for this mapper.
	 * @param pool thread pool to be used for this mapper.
	 */
	public void setThreadPool(ExecutorService pool) {
		this.threadPool = pool;
	}

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
    	Configuration conf = job.getConfiguration();
        return (Class<Mapper<K1, V1, K2, V2>>)conf.getClass(
        		ConfigConstants.CONF_MULTITHREADEDMAPPER_CLASS, Mapper.class);
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
     * @param internalMapperClass
     *            the class to use as the mapper
     */
    public static <K1, V1, K2, V2> void setMapperClass(Configuration conf,
        Class<? extends BaseMapper<?, ?, ?, ?>> internalMapperClass) {
        if (MultithreadedMapper.class.isAssignableFrom(internalMapperClass)) {
            throw new IllegalArgumentException("Can't have recursive "
                + "MultithreadedMapper instances.");
        }
        conf.setClass(ConfigConstants.CONF_MULTITHREADEDMAPPER_CLASS, internalMapperClass,
            Mapper.class);
    }

    /**
     * Run the application's maps using a thread pool.
     */
	@Override
    public void run(Context context) throws IOException, InterruptedException {
        outer = context;
        int numberOfThreads = getThreadCount(context);
        mapClass = getMapperClass(context);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Running with " + numberOfThreads + " threads");
        }
        // current mapper takes 1 thread
        numberOfThreads--;

        // submit runners
        try {
	        runners = new ArrayList<MapRunner>(numberOfThreads);
	        List<Future<?>> taskList = null;
	        if (threadPool != null) {
	        	taskList = new ArrayList<Future<?>>();
	            synchronized (threadPool) {
	                for (int i = 0; i < numberOfThreads; ++i) {
	                    MapRunner thread;
	                    thread = new MapRunner();
	                    if (!threadPool.isShutdown()) {
	                        Future<?> future = threadPool.submit(thread);
	                        taskList.add(future);
	                    } else {
	                        throw new InterruptedException(
	                            "Thread Pool has been shut down");
	                    }
	                }
	                threadPool.notify();
	            }
	            // MapRunner that runs in current thread
	            MapRunner r = new MapRunner();
	            r.run();
	
	            for (Future<?> f : taskList) {
	                f.get();
	            }
	        } else {
	            for (int i = 0; i < numberOfThreads; ++i) {
	                MapRunner thread;
	                thread = new MapRunner();
	                thread.start();
	                runners.add(i, thread);
	            }
	            // MapRunner runs in current thread
	            MapRunner r = new MapRunner();
	            r.run();
	            
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
        } catch (ClassNotFoundException e) {
            LOG.error("MapRunner class not found", e);
        } catch (ExecutionException e) {
        	LOG.error("Error waiting for MapRunner threads to complete", e);
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
            if (!outer.nextKeyValue()) {
                return false;
            }
            if (outer.getCurrentKey() == null) {
                return true;
            }
            key = (K1) ReflectionUtils.newInstance(outer.getCurrentKey()
                .getClass(), outer.getConfiguration());
            key = ReflectionUtils.copy(outer.getConfiguration(),
                outer.getCurrentKey(), key);
            value = (V1) ReflectionUtils.newInstance(outer.getCurrentValue()
                .getClass(), outer.getConfiguration());
            value = ReflectionUtils.copy(outer.getConfiguration(),
                outer.getCurrentValue(), value);
            return true;
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

        public float getProgress() {
            Method getProgressMethod;
            try {
                getProgressMethod = outer.getClass().getMethod(
                        "getProgress", new Class[0]);
                if (getProgressMethod != null) {
                    return (Float) getProgressMethod.invoke(outer, 
                            new Object[0]);
                }
            } catch (Exception e) {
                LOG.error("Error getting progress", e);
            }
            
            return 0;
        }

    }

    private class MapRunner extends Thread {
        private Mapper<K1, V1, K2, V2> mapper;
        private Context subcontext;
        private Throwable throwable;
        private RecordWriter<K2, V2> writer;

        MapRunner() throws IOException, InterruptedException, 
        ClassNotFoundException {
            // initiate the real mapper (DocumentMapper) that does the work
            mapper = ReflectionUtils.newInstance(mapClass,
                outer.getConfiguration());
            @SuppressWarnings("unchecked")
			OutputFormat<K2, V2> outputFormat = (OutputFormat<K2, V2>) 
			    ReflectionUtils.newInstance(outer.getOutputFormatClass(),
                outer.getConfiguration());
            writer = outputFormat.getRecordWriter(outer);
            try {
                subcontext = (Context)ReflectionUtil.createMapperContext(
                    mapper, outer.getConfiguration(), outer.getTaskAttemptID(),                   
                    new SubMapRecordReader(), writer,
                    outer.getOutputCommitter(), new SubMapStatusReporter(),
                    outer.getInputSplit());
            } catch (Exception e) {
                throw new IOException("Error creating mapper context", e);
            }
        }

        @Override
        public void run() {
            try {
                BaseMapper<K1, V1, K2, V2> real = 
                    (BaseMapper<K1, V1, K2, V2>) mapper;
                real.runThreadSafe(outer, subcontext);
                writer.close(subcontext);
            } catch (Throwable ie) {
                LOG.error(ie.getMessage(), ie);
                throwable = ie;
            }
        }
    }

}
