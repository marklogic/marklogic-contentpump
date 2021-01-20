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

import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XSInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import com.marklogic.contentpump.LocalJobRunner.LocalMapTask;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.InternalUtilities;

/**
 * Manages client side concurrency based on available server threads
 *
 * @author vzhang
 */
public class ThreadManager implements ConfigConstants {
    public static final Log LOG = LogFactory.getLog(ThreadManager.class);
    public static final String SERVER_MAX_THREADS_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n" +
        "let $f := " +
        "fn:function-lookup(xs:QName('hadoop:get-port-max-threads'),0)\n" +
        "return if (exists($f)) then $f() else 0";
    private LocalJob job;
    private Command cmd;
    private Configuration conf;
    private final ScheduledExecutorService scheduler;
    private ThreadPoolExecutor pool;
    private List<LocalMapTask> taskList = new ArrayList<>();
    private List<Future<Object>> taskFutureList = new ArrayList<>();
    // minimally required thread per task defined by the job
    private int minThreads = 1;
    private int curServerThreads;
    // Flag for indicating whether auto-scaling from last polling period is done
    private boolean isScalingDone = true;

    /**
     * Variables set by checkOutputFormat
     */
    private int newServerThreads;
    // Whether mlcp is running against a load balancer
    private boolean restrictHosts = false;

    /**
     * Command line options
     */
    private int threadCount;
    private int threadsPerSplit;
    private int maxThreads;
    private double maxThreadPercentage = 1;
    private int pollingInitDelay = 1;
    private int pollingPeriod = 1;


    public ThreadManager(LocalJob job) {
        this.job = job;
        this.conf = job.getConfiguration();
        this.minThreads = conf.getInt(CONF_MIN_THREADS, minThreads);
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    /**
     * Parse command line options
     * @param cmdline
     * @param cmd
     */
    public void parseCmdlineOptions(CommandLine cmdline, Command cmd) {
        this.cmd = cmd;
        // Parse command line options
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
        if (cmdline.hasOption(MAX_THREAD_PERCENTAGE)) {
            maxThreadPercentage = ((double)Integer.parseInt(
                cmdline.getOptionValue(MAX_THREAD_PERCENTAGE))) / 100;
        }
        if (cmdline.hasOption(POLLING_INIT_DELAY)) {
            pollingInitDelay = Integer.parseInt(
                cmdline.getOptionValue(POLLING_INIT_DELAY));
        }
        if (cmdline.hasOption(POLLING_PERIOD)) {
            pollingPeriod = Integer.parseInt(
                cmdline.getOptionValue(POLLING_PERIOD));
        }
    }

    /**
     * Query the server stack to get the maximum available thread count.
     * @param cs
     * @throws IOException
     */
    public void queryServerMaxThreads(ContentSource cs)
        throws IOException {
        Session session = null;
        ResultSequence result = null;
        try {
            session = cs.newSession();
            AdhocQuery query = session.newAdhocQuery(SERVER_MAX_THREADS_QUERY);
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            result = session.submitRequest(query);

            if (result.hasNext()) {
                ResultItem item = result.next();
                newServerThreads = (int)(maxThreadPercentage *
                    ((XSInteger)item.getItem()).asPrimitiveInt());
            } else {
                throw new IllegalStateException(
                    "Failed to query server max threads");
            }
        } catch (RequestException e) {
            LOG.error(e.getMessage(), e);
            throw new IOException(e);
        } finally {
            if (result != null) {
                result.close();
            }
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * Schedule thread polling tasks.
     */
    public void runThreadPoller() {
        final ScheduledFuture<?> handler =
            scheduler.scheduleAtFixedRate(new ThreadPoller(), pollingInitDelay,
                pollingPeriod, POLLING_TIME_UNIT);
    }

    /**
     * Check whether mlcp runs auto-scaling. If the user specifies command line
     * option -thread_count or -thread_count_per_split, or mlcp is not running
     * against a load balancer, then mlcp does not auto-scale.
     * @return true if mlcp runs auto-scaling, false if it doesn't.
     */
    public boolean runAutoScaling() {
        // Run auto-scaling when mlcp is run against load balancer, and
        // thread_count and threads_per_split are not specified.
        return (restrictHosts && (threadCount == 0) && (threadsPerSplit == 0));
    }

    /**
     * Initialize thread pool before mlcp starts running jobs.
     * @return initialized thread pool.
     */
    public ThreadPoolExecutor initThreadPool() {
        int numThreads;
        if (threadCount != 0) {
            // Use specified threadCount in the command line
            numThreads = threadCount;
        } else {
            // Use server max thread counts
            numThreads = newServerThreads;
            if (numThreads == 0) {
                // Mlcp export command or ML server version is below 10.0-4.2,
                // unable to get server thread count
                numThreads = DEFAULT_THREAD_COUNT;
            }
        }
        numThreads = Math.max(numThreads, minThreads);
        if (maxThreads > 0) {
            numThreads = Math.min(numThreads, maxThreads);
        }
        if (numThreads > 1) {
            pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(numThreads);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Initial thread pool size: " + numThreads);
                if (runAutoScaling()) {
                    LOG.debug("Thread pool will auto-scale based on " +
                        "available server threads.");
                } else {
                    LOG.debug("Thread pool is fixed and will not " +
                        "auto-scale.");
                }
            }
        }
        curServerThreads = numThreads;
        return pool;
    }

    /**
     * Scale-out thread pool based on newly available server threads. Create
     * new map runners and assign them to each existing LocalMapTask in a
     * round-robin fashion.
     */
    public void scaleOutThreadPool() {
        isScalingDone = false;
        if (maxThreads > 0 && newServerThreads > maxThreads) {
            LOG.debug("Thread count has reached the maximum value: " +
                maxThreads + " , and the thread pool will not further scale " +
                "out.");
            newServerThreads = maxThreads;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Thread pool is scaling-out. New thread pool " +
                    "size: " + newServerThreads);
            }
        }
        synchronized (pool) {
            pool.setMaximumPoolSize(newServerThreads);
            pool.setCorePoolSize(newServerThreads);
        }
        // Assign new available threads to each task
        for (int i = 0; i < taskList.size(); i++) {
            LocalMapTask task = taskList.get(i);
            int deltaTaskThreads = assignThreads(i, taskList.size(),
                (newServerThreads - curServerThreads));
            if (task.getMapperClass() == (Class)MultithreadedMapper.class) {
                int newTaskThreads = deltaTaskThreads + task.getThreadCount();
                task.setThreadCount(newTaskThreads);
                ((MultithreadedMapper)task.getMapper()).setThreadCount(
                    newTaskThreads);
                try {
                    // Create new map runners
                    ((MultithreadedMapper)task.getMapper()).createRunners(
                        deltaTaskThreads);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Running with MultithreadedMapper. New " +
                            "thread count for split #" + i + ": " +
                            newTaskThreads);
                    }
                }
                catch (ClassNotFoundException e) {
                    LOG.error("MapRunner class not found", e);
                } catch (IOException | InterruptedException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        isScalingDone = true;
    }

    /**
     * Scale-in thread pool based on new available server threads. Deduct
     * active runners from each LocalMapTask in a round-robin fashion.
     */
    public void scaleInThreadPool() {
        isScalingDone = false;
        if (newServerThreads < minThreads) {
            LOG.debug("Thread count has reached minimum value: " + minThreads
                + " and the thread pool will not further scale in.");
            newServerThreads = minThreads;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Thread pool is scaling-in. New thread pool " +
                    "size: " + newServerThreads);
            }
        }
        // Deduct runners from each task
        for (int i = 0; i < taskList.size(); i++) {
            LocalMapTask task = taskList.get(i);
            int deltaTaskThreads = assignThreads(i, taskList.size(),
                (curServerThreads - newServerThreads));
            if (task.getMapperClass() == (Class)MultithreadedMapper.class) {
                int newTaskThreads = task.getThreadCount() - deltaTaskThreads;
                // Stop currently running MapRunners
                ((MultithreadedMapper)task.getMapper()).stopRunners(
                    deltaTaskThreads);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Running with MultithreadedMapper. New " +
                        "thread count for split #" + i + ": " +
                        newTaskThreads);
                }
                task.setThreadCount(newTaskThreads);
                ((MultithreadedMapper)task.getMapper()).setThreadCount(
                    newTaskThreads);
            }
        }
        pool.setCorePoolSize(newServerThreads);
        pool.setMaximumPoolSize(newServerThreads);
        isScalingDone = true;
    }

    /**
     * Wait until all tasks are done and shutdown thread pool.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void shutdownThreadPool() throws InterruptedException,
        ExecutionException {
        // wait till all tasks are done
        for (Future<?> f : taskFutureList) {
            f.get();
        }
        if (pool != null) {
            pool.shutdown();
            while (!pool.awaitTermination(1, TimeUnit.HOURS));
        }
    }

    /**
     * Submit LocalMapTask (one task per input split) to thread pool.
     * @param task
     * @param index
     * @param splitCount
     * @throws Exception
     */
    public void submitTask(LocalMapTask task, int index, int splitCount)
        throws Exception {
        int taskThreads = assignThreads(index, splitCount, newServerThreads);
        Class<? extends Mapper<?,?,?,?>> mapperClass = job.getMapperClass();
        Class<? extends Mapper<?,?,?,?>> runtimeMapperClass = job.getMapperClass();
        // Possible runtime mapperClass adjustment. Use MultithreadedMapper for
        // each InputSplit assigned with only 1 thread as well for auto-scaling
        // purpose.
        if (taskThreads != threadsPerSplit) {
            runtimeMapperClass = cmd.getRuntimeMapperClass(job, mapperClass,
                threadsPerSplit);
            if (runtimeMapperClass != mapperClass) {
                task.setMapperClass(runtimeMapperClass);
            }
            if (runtimeMapperClass == (Class)MultithreadedMapper.class) {
                task.setThreadCount(taskThreads);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Running with MultithreadedMapper. Initial " +
                        "thread count for split #" + index + ": " +
                        taskThreads);
                }
            }
        }
        taskList.add(task);
        if (runtimeMapperClass == (Class)MultithreadedMapper.class) {
            synchronized (pool) {
                taskFutureList.add(pool.submit(task));
                pool.wait();
            }
        } else {
            pool.submit(task);
        }
    }

    /**
     * Assign thread count for a given split
     *
     * @param splitIndex split index
     * @param splitCount
     * @return number of threads for each input split
     */
    private int assignThreads(int splitIndex, int splitCount, int totalThreads) {
        if (threadsPerSplit > 0) {
            return threadsPerSplit;
        }
        if (splitCount == 1) {
            return totalThreads;
        }
        if (splitCount * minThreads > totalThreads) {
            return minThreads;
        }
        if (splitIndex % totalThreads < totalThreads % splitCount) {
            return totalThreads / splitCount + 1;
        } else {
            return totalThreads / splitCount;
        }
    }

    /**
     * Used by checkOutputSpecs for indicating whether mlcp is running against
     * a load balancer.
     * @param newRestrictHosts
     */
    public void setRestrictHosts(boolean newRestrictHosts) {
        restrictHosts = newRestrictHosts;
    }

    /**
     * Run server thread polling query and adjust thread pool
     */
    class ThreadPoller implements Runnable {
        private int pollingRetry;
        private int pollingSleepTime;
        private final int MAX_RETRIES = 5;
        private final int MIN_SLEEP_TIME = 500;

        @Override
        public void run() {
            if (ContentPump.shutdown) {
                scheduler.shutdown();
                return;
            }
            if (!runAutoScaling()) return;
            // Make sure that auto-scaling from the last polling period is done
            if (!isScalingDone) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("MLCP auto-scaling is not done yet, will " +
                        "run with thread pool size: " + curServerThreads +
                        " until it's done.");
                }
                return;
            }

            boolean succeeded = false;
            pollingRetry = 0;
            pollingSleepTime = MIN_SLEEP_TIME;
            // Poll server max threads
            while (pollingRetry < MAX_RETRIES) {
                if (pollingRetry > 0) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Retrying querying available server max threads.");
                    }
                }
                String[] hosts = conf.getStrings(MarkLogicConstants.OUTPUT_HOST);
                for (int i = 0; i < hosts.length; i++) {
                    try {
                        ContentSource cs = InternalUtilities.
                            getOutputContentSource(conf, hosts[i]);
                        queryServerMaxThreads(cs);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("New available server threads: " +
                                newServerThreads);
                        }
                        succeeded = true;
                        break;
                    } catch (Exception e) {
                        if (e.getCause() instanceof ServerConnectionException) {
                            LOG.warn("Unable to connect to " + hosts[i]
                                + " to query available server max threads.");
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(e);
                            }
                        } else {
                            LOG.error(e.getMessage(), e);
                        }
                    }
                }
                if (succeeded) break;
                if (++pollingRetry < MAX_RETRIES) {
                    sleep();
                } else {
                    LOG.error("Exceed max querying retry. Unable to query" +
                        "available server max threads.");
                    job.setJobState(JobStatus.State.FAILED);
                    return;
                }
            }
            if (curServerThreads < newServerThreads) {
                // Scale out
                scaleOutThreadPool();
            } else if (curServerThreads > newServerThreads) {
                // Scale in
                scaleInThreadPool();
            } else return;
            curServerThreads = newServerThreads;
        }

        private void sleep() {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sleeping before retrying...sleepTime= " +
                        pollingSleepTime + "ms");
                }
                InternalUtilities.sleep(pollingSleepTime);
            } catch (Exception e) {}
            pollingSleepTime = pollingSleepTime * 2;
        }
    }
}
