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

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.sqoop.util.OptionsFileUtil;

/**
 * ContentPump entry point.  MarkLogic ContentPump is a tool that moves content 
 * between a MarkLogic database and file system or copies content from one 
 * MarkLogic database to another.
 * 
 * @author jchen
 *
 */
public class ContentPump implements ConfigConstants {
    
    public static final Log LOG = LogFactory.getLog(ContentPump.class.getName());
    
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }
        
        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception ex) {
            LOG.error("Error while expanding arguments", ex);
            System.err.println(ex.getMessage());
            System.err.println("Try 'mloader help' for usage.");
        }
        
        runCommand(expandedArgs);
    }

    private static void runCommand(String[] args) 
    throws Exception {
        // get command
        String cmd = args[0];
        if (cmd.equals("help")) {
            printUsage();
            return;
        }
        Command command = Command.forName(cmd);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Command: " + command);
        }
        
        // get options arguments
        String[] optionArgs = Arrays.copyOfRange(args, 1, args.length);
        
        // parse hadoop specific options
        Configuration conf = new Configuration();
        GenericOptionsParser genericParser = new GenericOptionsParser(
                conf, optionArgs);
        String[] remainingArgs = genericParser.getRemainingArgs();
        
        // parse command specific options
        Options options = new Options();
        command.configOptions(options);
        CommandLineParser parser = new GnuParser();
        CommandLine cmdline = parser.parse(options, remainingArgs);
      
        // create job
        Job job = command.createJob(conf, cmdline);
        
        // run job
        String mode = cmdline.getOptionValue(MODE);
        if (cmdline.hasOption(HADOOP_HOME) ||
            System.getProperty(HADOOP_HOME_PROPERTY_NAME) != null) {
            if (mode == null || mode.equalsIgnoreCase(MODE_DISTRIBUTED)) {
                // set HADOOP_HOME based on cmdline setting
                if (cmdline.hasOption(HADOOP_HOME)) {
                    System.setProperty(HADOOP_HOME_PROPERTY_NAME, 
                            cmdline.getOptionValue(HADOOP_HOME));
                }
                // submit job
                job.waitForCompletion(true);
            } else if (mode.equalsIgnoreCase(MODE_LOCAL)) {
                LocalJobRunner runner = new LocalJobRunner(job, cmdline);
                runner.run();
            } else {
                throw new IllegalArgumentException("Invalid mode " 
                        + mode);
            }
        } else {
            if (mode == null || mode.equalsIgnoreCase(MODE_LOCAL)) {
                LocalJobRunner runner = new LocalJobRunner(job, cmdline);
                runner.run();
            } else if (mode.equalsIgnoreCase(MODE_DISTRIBUTED)) {
                throw new IllegalArgumentException("Missing configuration: " +
                        HADOOP_HOME);
            } else {
                throw new IllegalArgumentException("Invalid mode " 
                        + mode);
            }
        }
    }

    private static void printUsage() {
        // TODO Auto-generated method stub
        
    }
}
