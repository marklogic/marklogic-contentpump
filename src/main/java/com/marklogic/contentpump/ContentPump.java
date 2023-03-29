/*
 * Copyright (c) 2021 MarkLogic Corporation
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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.VersionInfo;

import com.marklogic.contentpump.utilities.AuditUtil;
import com.marklogic.contentpump.utilities.CommandlineOptions;
import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * ContentPump entry point.  MarkLogic ContentPump is a tool that moves content 
 * between a MarkLogic database and file system or copies content from one 
 * MarkLogic database to another.
 * 
 * @author jchen
 *
 */
public class ContentPump implements MarkLogicConstants, ConfigConstants {
    
    public static final Log LOG = LogFactory.getLog(ContentPump.class);
    // A global flag to indicate that JVM is exiting
    public static volatile boolean shutdown = false;
    // Job state in local mode
    static List<Job> jobs = new LinkedList<>();
    static {
        // register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }
        
        String[] expandedArgs = null;
        int rc = 1;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
            rc = runCommand(expandedArgs);
        } catch (Exception ex) {
            LOG.error("Error while expanding arguments", ex);
            System.err.println(ex.getMessage());
            System.err.println("Try 'mlcp help' for usage.");
        }
        
        System.exit(rc);
    }

    public static int runCommand(String[] args) throws IOException {      
        // get command
        String cmd = args[0];
        if (cmd.equalsIgnoreCase("help")) {
            printUsage();
            return 1;
        } else if (cmd.equalsIgnoreCase("version")) {
            logVersions();
            return 1;
        }
        
        Command command = Command.forName(cmd);

        // get options arguments
        String[] optionArgs = Arrays.copyOfRange(args, 1, args.length);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Command: " + command);
            StringBuilder buf = new StringBuilder();
            // Password masking
            boolean isPassword = false;
            for (String arg : optionArgs) {
                if (isPassword) {
                    arg = "...";
                    isPassword = false;
                }
                if (arg.matches(".*password")) {
                    isPassword = true;
                }
                buf.append(arg);
                buf.append(' ');
            }     
            LOG.debug("Arguments: " + buf);
        }
        
        // parse hadoop specific options
        Configuration conf = new Configuration();
        GenericOptionsParser genericParser = new GenericOptionsParser(
                conf, optionArgs);
        String[] remainingArgs = genericParser.getRemainingArgs();
        
        // parse command specific options
        CommandlineOptions options = new CommandlineOptions();
        command.configOptions(options);
        CommandLineParser parser = new GnuParser();
        CommandLine cmdline;
        try {
            cmdline = parser.parse(options, remainingArgs);
        } catch (Exception e) {
            LOG.error("Error parsing command arguments: ");
            LOG.error(e.getMessage());
            // Print the command usage message and exit.    
            command.printUsage(command, options.getPublicOptions());
            return 1; // Exit on exception here.
        }

        for (String arg : cmdline.getArgs()) {
            LOG.error("Unrecognized argument: " + arg);
            // Print the command usage message and exit.
            command.printUsage(command, options.getPublicOptions());
            return 1; // Exit on exception here.
        }
        
        // check running mode and hadoop conf dir configuration 
        String mode = cmdline.getOptionValue(MODE);
        if (mode != null &&
                !MODE_DISTRIBUTED.equalsIgnoreCase(mode) &&
                !MODE_LOCAL.equalsIgnoreCase(mode)) {
            LOG.error("Unrecognized option argument for " +
                MODE + ": " + mode);
            return 1;
        }
        String hadoopConfDir = System.getenv(HADOOP_CONFDIR_ENV_NAME);
        if (cmdline.hasOption(HADOOP_CONF_DIR)) {
            hadoopConfDir = cmdline.getOptionValue(HADOOP_CONF_DIR);
        }
        
        boolean distributed = hadoopConfDir != null && (mode == null ||
                mode.equalsIgnoreCase(MODE_DISTRIBUTED));
        if (MODE_DISTRIBUTED.equalsIgnoreCase(mode) && !distributed) {
            LOG.error("Cannot run in distributed mode.  HADOOP_CONF_DIR is "
                    + "not configured.");
        }
        if (distributed) {
            LOG.error("MLCP distributed mode has been disabled. Please use local mode instead.");
            return 1;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Running in: " + (distributed ? "distributed " : "local")
                + "mode");
            if (distributed) {
                LOG.debug("HADOOP_CONF_DIR is set to " + hadoopConfDir);
            }
        }
        conf.set(EXECUTION_MODE, distributed ? MODE_DISTRIBUTED : MODE_LOCAL);
        
        if (conf.getBoolean(RESTRICT_INPUT_HOSTS, true) ||
                conf.getBoolean(RESTRICT_OUTPUT_HOSTS, true)) {
            System.setProperty("xcc.httpcompliant", "true");
        }

        if (distributed) {
            if (!cmdline.hasOption(SPLIT_INPUT)
                && Command.getInputType(cmdline).equals(
                    InputType.DELIMITED_TEXT)) {
                conf.setBoolean(ConfigConstants.CONF_SPLIT_INPUT, true);
            }
            File hdConfDir= new File(hadoopConfDir);
            try { 
                checkHadoopConfDir(hdConfDir);
            } catch (IllegalArgumentException e) {
                LOG.error("Error found with Hadoop home setting", e);
                System.err.println(e.getMessage());
                return 1;
            }
            // set new class loader based on Hadoop Conf Dir
            try {
                setClassLoader(hdConfDir, conf);
            } catch (Exception e) {
                LOG.error("Error configuring class loader", e);
                System.err.println(e.getMessage());
                return 1;
            }
        } else { // running in local mode
            String bundleVersion = 
                    System.getProperty(CONTENTPUMP_BUNDLE_ARTIFACT);
            if (bundleVersion != null && 
                    "mapr".equals(bundleVersion)) {
                String cpHome = 
                        System.getProperty(CONTENTPUMP_HOME_PROPERTY_NAME);
                if (cpHome != null) {
                    System.setProperty("java.security.auth.login.config", cpHome + "mapr.conf");
                }
            }
            // Tell Hadoop that we are running in local mode.  This is useful
            // when the user has Hadoop home or their Hadoop conf dir in their
            // classpath but want to run in local mode.
            conf.set(CONF_MAPREDUCE_JOBTRACKER_ADDRESS, "local");
        }
                
        // create job
        Job job = null;
        try { 
            if (distributed) {
                // So far all jobs created by mlcp are map only,
                // so set number of reduce tasks to 0.
                conf.setInt("mapreduce.job.reduces", 0);
                // No speculative runs since speculative tasks don't get to 
                // clean up sessions properly
                conf.setBoolean("mapreduce.map.speculative",
                                false);
            } else {
                // set working directory
                conf.set(CONF_MAPREDUCE_JOB_WORKING_DIR, System.getProperty("user.dir"));
            }
            job = command.createJob(conf, cmdline);
            LOG.info("Job name: " + job.getJobName());
        } catch (Exception e) {
            // Print exception message.
            e.printStackTrace();
            return 1;
        }
        synchronized (ContentPump.class) {
            jobs.add(job);
        }
        
        // run job
        try {
            if (distributed) {
                // submit job
                submitJob(job);
            } else {                
                runJobLocally((LocalJob)job, cmdline, command);
            }
            return getReturnCode(job.getJobState());
        } catch (Exception e) {
            LOG.error("Error running a ContentPump job", e); 
            e.printStackTrace(System.err);
            return 1;
        }
    }
    
    /**
     * Set class loader for current thread and for Confifguration based on 
     * Hadoop home.
     * 
     * @param hdConfDir Hadoop home directory
     * @param conf Hadoop configuration
     * @throws MalformedURLException
     */
    private static void setClassLoader(File hdConfDir, Configuration conf) 
    throws Exception {
        ClassLoader parent = conf.getClassLoader();
        URL url = hdConfDir.toURI().toURL();
        URL[] urls = new URL[1];
        urls[0] = url;
        ClassLoader classLoader = new URLClassLoader(urls, parent);
        Thread.currentThread().setContextClassLoader(classLoader);
        conf.setClassLoader(classLoader);
    }

    private static void checkHadoopConfDir(File hdConfDir) 
    throws IllegalArgumentException {
        if (!hdConfDir.exists()) {
            throw new IllegalArgumentException("Hadoop conf dir " + hdConfDir 
                    + " is not found.");
        } else if (!hdConfDir.isDirectory()) {
            throw new IllegalArgumentException("Hadoop conf dir " + hdConfDir 
                    + " is not a directory.");
        } else if (!hdConfDir.canRead()) {
            throw new IllegalArgumentException("Hadoop conf dir " + hdConfDir
                    + " cannot be read.");
        }
     }

    private static void submitJob(Job job) throws Exception {
        String cpHome = 
            System.getProperty(CONTENTPUMP_HOME_PROPERTY_NAME);
        
        // find job jar
        File cpHomeDir= new File(cpHome);
        FilenameFilter jobJarFilter = new FilenameFilter() {
        	@Override
            public boolean accept(File dir, String name) {
              return name.endsWith(".jar") &&
                  name.startsWith(CONTENTPUMP_JAR_PREFIX);
            }
        };
        File[] cpJars = cpHomeDir.listFiles(jobJarFilter);
        if (cpJars == null || cpJars.length == 0) {
        	throw new RuntimeException("Content Pump jar file " + 
                 "is not found under " + cpHome);
        }
        if (cpJars.length > 1) {
        	throw new RuntimeException("More than one Content Pump jar file " +
                     "are found under " + cpHome);
        }
        // set job jar
        Configuration conf = job.getConfiguration();       
        conf.set("mapreduce.job.jar", cpJars[0].toURI().toURL().toString());

        // find lib jars
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar") && !name.startsWith("hadoop");
            }

        };

        // set lib jars
        StringBuilder jars = new StringBuilder();
        for (File jar : cpHomeDir.listFiles(filter)) {
            if (jars.length() > 0) {
                jars.append(',');
            }
            jars.append(jar.toURI().toURL().toString());
        }
        conf.set("tmpjars", jars.toString());
        if (LOG.isTraceEnabled())
            LOG.trace("LIBJARS:" + jars.toString());
        job.waitForCompletion(true);
        AuditUtil.auditMlcpFinish(conf, job.getJobName(), job.getCounters());
        
        synchronized(ContentPump.class) {
            jobs.remove(job);
            ContentPump.class.notify();
        }
    }
    
    private static void runJobLocally(LocalJob job, CommandLine cmdline,
            Command cmd) throws Exception {
        LocalJobRunner runner = new LocalJobRunner(job, cmdline, cmd);
        runner.run();
        AuditUtil.auditMlcpFinish(job.getConfiguration(),
                job.getJobName(), runner.getReporter().counters);
        
        synchronized(ContentPump.class) {
            jobs.remove(job);
            ContentPump.class.notify();
        }
    }

    private static void printUsage() {
        System.out.println("usage: mlcp COMMAND [ARGS]\n");
        System.out.println("Available commands:");
        System.out.println("  IMPORT  import data to a MarkLogic database");
        System.out.println("  EXPORT  export data from a MarkLogic database");
        System.out.println("  COPY    copy data from one MarkLogic database to another");
        System.out.println("  EXTRACT extract data from MarkLogic forests");
        System.out.println("  HELP    list available commands");
        System.out.println("  VERSION print the version");
    }
    
    public static void logVersions() {
        System.out.println("ContentPump version: " + Versions.getVersion());
        System.out.println("Java version: " + 
            System.getProperty("java.version"));
        System.out.println("Hadoop version: " + VersionInfo.getVersion());
        System.out.println("Supported MarkLogic versions: " + 
                Versions.getMinServerVersion() + " - " + 
                Versions.getMaxServerVersion());
    }
    
    public static int getReturnCode(JobStatus.State state) {
        switch (state) {
            case RUNNING:
                return -1;
            case SUCCEEDED:
                return 0;
            case FAILED:
                return 1;
            case PREP:
                return 2;
            case KILLED:
                return 3;
            default:
                return 4;
        }
    }

    static class ShutdownHook extends Thread { 
        boolean needToWait() {
            boolean needToWait = false;
            for (Job job : jobs) {
                if (job instanceof LocalJob) {
                    needToWait = true;
                    break;
                }
            } 
            return needToWait;
        }
        
        public void run() {
            shutdown = true;
            // set system property for hadoop connector classes
            System.setProperty("mlcp.shutdown", "1");
            try {
                synchronized (ContentPump.class) {
                    boolean needToWait = false;
                    List<Job> jobList = new LinkedList<>();
                    for (Job job : jobs) {
                        if (job instanceof LocalJob) {
                            LOG.info("Aborting job " + job.getJobName());
                            needToWait = true;
                            jobList.add(job);
                        }
                    }
                    if (needToWait) { // wait up to 30 seconds
                        for (int i = 0; i < 30; ++i) {
                            if (i > 0) { // check again
                                needToWait = needToWait();
                            }
                            if (needToWait) {
                                if (i > 0) {
                                    LOG.info("Waiting..." + (30-i));
                                }
                                try {
                                    ContentPump.class.wait(1000);
                                } catch (InterruptedException e) {
                                    // go back to wait
                                }
                            }  
                        }
                    }
                    for (Job job : jobs) {
                        LOG.warn("Job " + job.getJobName() +
                                " status remains " + job.getJobState()); 
                        jobList.remove(job);
                    }
                    for (Job job : jobList) {
                        LOG.warn("Job " + job.getJobName() + " is aborted");
                    }
                }
            } catch (Exception e) {
                LOG.error("Error terminating job", e);
            }
        }
    }
}
