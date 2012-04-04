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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * Enum of supported commands.
 * 
 * @author jchen
 */
@SuppressWarnings("static-access")
public enum Command implements ConfigConstants {
    IMPORT {     
        @Override
        public void configOptions(Options options) {
            configCommonOptions(options);
            Option inputFilePath = OptionBuilder.withArgName(INPUT_FILE_PATH)
                .hasArg()
                .withDescription("The file system path in which to look for " +
                    "input")
                .create(INPUT_FILE_PATH);
            options.addOption(inputFilePath);
            Option inputFilePattern = 
                OptionBuilder.withArgName(INPUT_FILE_PATTERN)
                .hasArg()
                .withDescription("Matching regex pattern for files found in " +
                    "the input file path")
                .create(INPUT_FILE_PATH);
            options.addOption(inputFilePattern);
            Option inputRecordName = 
                OptionBuilder.withArgName(INPUT_RECORD_NAME)
                .hasArg()
                .withDescription("Element name in which each document is " +
                    "found")
                .create(INPUT_RECORD_NAME);
            options.addOption(inputRecordName);
            Option inputRecordNamespace = 
                OptionBuilder.withArgName(INPUT_RECORD_NAMESPACE)
                .hasArg()
                .withDescription("Element namespace in which each document " +
                    "is found")
                .create(INPUT_RECORD_NAMESPACE);
            options.addOption(inputRecordNamespace);
            Option optionalMetadata = 
                OptionBuilder.withArgName(INPUT_METADATA_OPTIONAL)
                .hasArg()
                .withDescription("Whether metadata files are optional in " +
                    "input")
                .create(INPUT_METADATA_OPTIONAL);
            options.addOption(optionalMetadata);
            Option inputFileType = 
                OptionBuilder.withArgName(INPUT_TYPE)
                .hasArg()
                .withDescription("Type of input file.  Valid choices are: " +
                    "documents, XML aggregates, delimited text, and export " +
                    "archive.")
                .create(INPUT_TYPE);
            options.addOption(inputFileType);
            Option inputCompressed = 
                OptionBuilder.withArgName(INPUT_COMPRESSED)
                .hasArg()
                .withDescription("Whether the input data is compressed")
                .create(INPUT_COMPRESSED);
            options.addOption(inputCompressed);
            Option inputCompressionCodec = 
                OptionBuilder.withArgName(INPUT_COMPRESSION_CODEC)
                .hasArg()
                .withDescription("Codec used for compression")
                .create(INPUT_COMPRESSION_CODEC);
            options.addOption(inputCompressionCodec);
            //TODO: complete
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline) 
        throws IOException {
            // apply configuration
            String inputTypeOption = INPUT_TYPE_DEFAULT;
            if (cmdline.hasOption(INPUT_TYPE)) {
                inputTypeOption = cmdline.getOptionValue(INPUT_TYPE);
            }
            InputType type = InputType.forName(inputTypeOption);
            String documentType = conf.get(MarkLogicConstants.CONTENT_TYPE, 
                    MarkLogicConstants.DEFAULT_CONTENT_TYPE);
            if (cmdline.hasOption(DOCUMENT_TYPE)) {
                documentType = cmdline.getOptionValue(DOCUMENT_TYPE);
            }
            ContentType contentType = ContentType.forName(documentType);
            
            // construct a job
            Job job = new Job(conf);
            job.setJarByClass(this.getClass());
            job.setInputFormatClass(type.getInputFormatClass(contentType));
            job.setMapperClass(type.getMapperClass(contentType));
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputFormatClass(ContentOutputFormat.class);
            
            if (cmdline.hasOption(INPUT_FILE_PATH)) {
                FileInputFormat.setInputPaths(job, 
                        cmdline.getOptionValue(INPUT_FILE_PATH));
            }      
            
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, 
                CommandLine cmdline) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void printUsage() {
            // TODO Auto-generated method stub
            
        }   
        
    },
    EXPORT {
        @Override
        public void configOptions(Options options) {
            configCommonOptions(options);
            //TODO: complete
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void applyConfigOptions(Configuration conf, 
                CommandLine cmdline) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void printUsage() {
            // TODO Auto-generated method stub
            
        }
        
    },
    COPY {
        @Override
        public void configOptions(Options options) {
            configCommonOptions(options);
            //TODO: complete
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void applyConfigOptions(Configuration conf, 
                CommandLine cmdline) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void printUsage() {
            // TODO Auto-generated method stub
            
        }       
    };
    
    public static Command forName(String cmd) {
        if (cmd.equalsIgnoreCase(IMPORT.name())) {
            return IMPORT;
        } else if (cmd.equalsIgnoreCase(EXPORT.name())) { 
            return EXPORT;
        } else if (cmd.equalsIgnoreCase(COPY.name())) {
            return COPY;
        } else {
            throw new IllegalArgumentException("Unknown command: " + cmd);
        }
    }

    /**
     * Add supported config options.
     * 
     * @param options
     */
    public abstract void configOptions(Options options);
    
    /**
     * Create a job based on Hadoop configuration and options.
     * 
     * @param conf Hadoop configuration
     * @param options options
     * @return a Hadoop job
     * @throws Exception 
     */
    public abstract Job createJob(Configuration conf, CommandLine cmdline) 
    throws IOException;
    
    /**
     * Apply config options set from the command-line to the configuration.
     * 
     * @param conf configuration
     * @param cmdline command line options
     */
    public abstract void applyConfigOptions(Configuration conf, 
            CommandLine cmdline);
    
    static void configCommonOptions(Options options) {
        Option mode = OptionBuilder.withArgName(MODE)
            .hasArg()
            .withDescription("Whether to run in single client or distributed.")
            .create(MODE);
        options.addOption(mode);  
        Option hadoopHome = OptionBuilder.withArgName(HADOOP_HOME)
            .hasArg()
            .withDescription("Override $HADOOP_HOME")
            .create(HADOOP_HOME);
        options.addOption(hadoopHome);
        Option threadCount = OptionBuilder.withArgName(THREAD_COUNT)
            .hasArg()
            .withDescription("Number of threads")
            .create(THREAD_COUNT);
        options.addOption(threadCount);
        Option splitSize = OptionBuilder.withArgName(MAX_SPLIT_SIZE)
            .hasArg()
            .withDescription("Maximum number of records per each split")
            .create(MAX_SPLIT_SIZE);
        options.addOption(splitSize);
    }

    public abstract void printUsage();
}
