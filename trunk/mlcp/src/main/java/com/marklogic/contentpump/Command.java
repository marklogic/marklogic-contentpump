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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentType;
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
            Option aggregateRecordElement = 
                OptionBuilder.withArgName(AGGREGATE_RECORD_ELEMENT)
                .hasArg()
                .withDescription("Element name in which each document is " +
                    "found")
                .create(AGGREGATE_RECORD_ELEMENT);
            options.addOption(aggregateRecordElement);
            Option aggregateRecordNamespace = 
                OptionBuilder.withArgName(AGGREGATE_RECORD_NAMESPACE)
                .hasArg()
                .withDescription("Element namespace in which each document " +
                    "is found")
                .create(AGGREGATE_RECORD_NAMESPACE);
            options.addOption(aggregateRecordNamespace);
            Option aggregateUriId = 
                OptionBuilder.withArgName(AGGREGATE_URI_ID)
                .hasArg()
                .withDescription("Element namespace in which each document " +
                    "is found")
                .create(AGGREGATE_URI_ID);
            options.addOption(aggregateUriId);
            Option optionalMetadata = 
                OptionBuilder.withArgName(INPUT_METADATA_OPTIONAL)
                .hasArg()
                .withDescription("Whether metadata files are optional in " +
                    "input")
                .create(INPUT_METADATA_OPTIONAL);
            options.addOption(optionalMetadata);
            Option inputFileType = 
                OptionBuilder.withArgName(INPUT_FILE_TYPE)
                .hasArg()
                .withDescription("Type of input file.  Valid choices are: " +
                    "documents, XML aggregates, delimited text, and export " +
                    "archive.")
                .create(INPUT_FILE_TYPE);
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
            Option documentType = OptionBuilder
                .withArgName(DOCUMENT_TYPE)
                .hasArg()
                .withDescription(
                    "Type of document content. Valid choices: " +
                    "XML, TEXT, BINARY")
                .create(DOCUMENT_TYPE);
            options.addOption(documentType);
            Option delimiter = OptionBuilder.withArgName(DELIMITER).hasArg()
                .withDescription("Delimiter for delimited text.")
                .create(DELIMITER);
            options.addOption(delimiter);
            Option delimitedUri = OptionBuilder.withArgName(DELIMITED_URI_ID)
                .hasArg()
                .withDescription("Delimited uri id for delimited text.")
                .create(DELIMITED_URI_ID);
            options.addOption(delimitedUri);
            Option outputUriReplace = OptionBuilder
                .withArgName(OUTPUT_URI_REPLACE).hasArg()
                .withDescription("Replace URI in output.")
                .create(OUTPUT_URI_REPLACE);
            options.addOption(outputUriReplace);
            Option outputUriPrefix = OptionBuilder
                .withArgName(OUTPUT_URI_PREFIX).hasArg()
                .withDescription("Prefix added to URI in output.")
                .create(OUTPUT_URI_PREFIX);
            options.addOption(outputUriPrefix);
            Option outputUriSuffix = OptionBuilder
                .withArgName(OUTPUT_URI_SUFFIX).hasArg()
                .withDescription("Suffix added to URI in output.")
                .create(OUTPUT_URI_SUFFIX);
            options.addOption(outputUriSuffix);
            Option outputFilenameCollection = OptionBuilder
                .withArgName(OUTPUT_FILENAME_AS_COLLECTION).hasArg()
                .withDescription("Filename as collection in output.")
                .create(OUTPUT_FILENAME_AS_COLLECTION);
            options.addOption(outputFilenameCollection);
            Option outputCollections = OptionBuilder
                .withArgName(OUTPUT_COLLECTIONS).hasArg()
                .withDescription("Output collections in output.")
                .create(OUTPUT_COLLECTIONS);
            options.addOption(outputCollections);
            Option outputPermissions = OptionBuilder
                .withArgName(OUTPUT_PERMISSIONS).hasArg()
                .withDescription("Output permissions in output.")
                .create(OUTPUT_PERMISSIONS);
            options.addOption(outputPermissions);
            Option outputQuantity = OptionBuilder.withArgName(OUTPUT_QUALITY)
                .hasArg().withDescription("Output quantity in output.")
                .create(OUTPUT_QUALITY);
            options.addOption(outputQuantity);
            Option outputCleanDir = OptionBuilder.withArgName(OUTPUT_CLEANDIR)
                .hasArg()
                .withDescription("Whether to clean dir before output.")
                .create(OUTPUT_CLEANDIR);
            options.addOption(outputCleanDir);
            Option batchSize = OptionBuilder.withArgName(BATCH_SIZE).hasArg()
                .withDescription("Batch size.").create(BATCH_SIZE);
            options.addOption(batchSize);
            Option txnSize = OptionBuilder.withArgName(TRANSACTION_SIZE)
                .hasArg().withDescription("Transaction size.")
                .create(TRANSACTION_SIZE);
            options.addOption(txnSize);
            Option outputLanguage = OptionBuilder.withArgName(OUTPUT_LANGUAGE)
                .hasArg().withDescription("Output language.")
                .create(OUTPUT_LANGUAGE);
            options.addOption(outputLanguage);
            //TODO: complete
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline) 
        throws IOException {
            applyConfigOptions(conf, cmdline);
            
            String inputTypeOption = INPUT_FILE_TYPE_DEFAULT;
            if (cmdline.hasOption(INPUT_FILE_TYPE)) {
                inputTypeOption = cmdline.getOptionValue(INPUT_FILE_TYPE);
            }
            InputType type = InputType.forName(inputTypeOption);
            String documentType = conf.get(MarkLogicConstants.CONTENT_TYPE,
                MarkLogicConstants.DEFAULT_CONTENT_TYPE);
            ContentType contentType = ContentType.forName(documentType);
            boolean compressed = ConfigConstants.DEFAULT_INPUT_COMPRESSED;
            if (cmdline.hasOption(INPUT_COMPRESSED)) {
                compressed = Boolean.parseBoolean(cmdline
                    .getOptionValue(INPUT_COMPRESSED));
            }
            
            // construct a job
            Job job = new Job(conf);
            job.setJarByClass(this.getClass());
            job.setInputFormatClass(type.getInputFormatClass(contentType,
                    compressed));
            job.setMapperClass(type.getMapperClass(contentType));
            job.setOutputFormatClass(ContentOutputFormat.class);
            
            if (cmdline.hasOption(INPUT_FILE_PATH)) {
                FileInputFormat.setInputPaths(job, 
                        cmdline.getOptionValue(INPUT_FILE_PATH));
            }      
            
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            if (cmdline.hasOption(DOCUMENT_TYPE)) {
                String documentType = cmdline.getOptionValue(DOCUMENT_TYPE);
                conf.set(MarkLogicConstants.CONTENT_TYPE,
                    documentType.toUpperCase());
            }
            if (cmdline.hasOption(INPUT_COMPRESSION_CODEC)) {
                String codec = cmdline.getOptionValue(INPUT_COMPRESSION_CODEC);
                conf.set(CONF_INPUT_COMPRESSION_CODEC, codec.toUpperCase());
            }
            if (cmdline.hasOption(MAX_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE);
                conf.set(CONF_MAX_SPLIT_SIZE, maxSize);
            }
            if (cmdline.hasOption(MIN_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MIN_SPLIT_SIZE);
                conf.set(CONF_MIN_SPLIT_SIZE, maxSize);
            }
            if (cmdline.hasOption(AGGREGATE_URI_ID)) {
                String uriId = cmdline.getOptionValue(AGGREGATE_URI_ID);
                conf.set(CONF_AGGREGATE_URI_ID, uriId);
            }
            if (cmdline.hasOption(AGGREGATE_RECORD_ELEMENT)) {
                String recElem = cmdline
                    .getOptionValue(AGGREGATE_RECORD_ELEMENT);
                conf.set(CONF_AGGREGATE_RECORD_ELEMENT, recElem);
            }
            if (cmdline.hasOption(AGGREGATE_RECORD_NAMESPACE)) {
                String recNs = cmdline
                    .getOptionValue(AGGREGATE_RECORD_NAMESPACE);
                conf.set(CONF_AGGREGATE_RECORD_NAMESPACE, recNs);
            }
            if (cmdline.hasOption(DELIMITER)) {
                String delim = cmdline.getOptionValue(DELIMITER);
                conf.set(CONF_DELIMITER, delim);
            }
            if (cmdline.hasOption(DELIMITED_URI_ID)) {
                String delimId = cmdline.getOptionValue(DELIMITED_URI_ID);
                conf.set(CONF_DELIMITED_URI_ID, delimId);
            }
            if (cmdline.hasOption(OUTPUT_DIRECTORY)) {
                String outDir = cmdline.getOptionValue(OUTPUT_DIRECTORY);
                conf.set(CONF_OUTPUT_DIRECTORY, outDir);
            }
            if (cmdline.hasOption(OUTPUT_URI_REPLACE)) {
                String[] uriReplace = cmdline
                    .getOptionValues(OUTPUT_URI_REPLACE);
                if (uriReplace != null && uriReplace.length == 2) {
                    LOG.error(OUTPUT_URI_REPLACE
                        + "is not configured correctly.");
                }
                conf.setStrings(CONF_OUTPUT_URI_REPLACE, uriReplace);
            }
            if (cmdline.hasOption(OUTPUT_URI_PREFIX)) {
                String outPrefix = cmdline.getOptionValue(OUTPUT_URI_PREFIX);
                conf.set(CONF_OUTPUT_URI_PREFIX, outPrefix);
            }
            if (cmdline.hasOption(OUTPUT_URI_SUFFIX)) {
                String outSuffix = cmdline.getOptionValue(OUTPUT_URI_SUFFIX);
                conf.set(CONF_OUTPUT_URI_SUFFIX, outSuffix);
            }
            if(cmdline.hasOption(OUTPUT_COLLECTIONS)) {
                String collectionsString = cmdline.getOptionValue(OUTPUT_COLLECTIONS);
                conf.set(MarkLogicConstants.OUTPUT_COLLECTION, collectionsString);
            }
            if(cmdline.hasOption(OUTPUT_PERMISSIONS)) {
                String permissionString = cmdline.getOptionValue(OUTPUT_PERMISSIONS);
                conf.set(MarkLogicConstants.OUTPUT_PERMISSION, permissionString);
            }
            if(cmdline.hasOption(OUTPUT_QUALITY)) {
                String quantity = cmdline.getOptionValue(OUTPUT_QUALITY);
                conf.set(MarkLogicConstants.OUTPUT_QUALITY, quantity);
            }
            if(cmdline.hasOption(OUTPUT_CLEANDIR)) {
                String cleandir = cmdline.getOptionValue(OUTPUT_CLEANDIR);
                conf.set(MarkLogicConstants.OUTPUT_CLEAN_DIR, cleandir);
            }
            if(cmdline.hasOption(BATCH_SIZE)) {
                String batchSize = cmdline.getOptionValue(BATCH_SIZE);
                conf.set(MarkLogicConstants.BATCH_SIZE, batchSize);
            }
            if(cmdline.hasOption(TRANSACTION_SIZE)) {
                String txnSize = cmdline.getOptionValue(TRANSACTION_SIZE);
                conf.set(MarkLogicConstants.TXN_SIZE, txnSize);
            }
            if(cmdline.hasOption(OUTPUT_LANGUAGE)) {
                String language = cmdline.getOptionValue(OUTPUT_LANGUAGE);
                conf.set(MarkLogicConstants.OUTPUT_CONTENT_LANGUAGE, language);
            }
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
