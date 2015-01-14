/*
 * Copyright 2003-2015 MarkLogic Corporation
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
import java.nio.charset.Charset;

import com.marklogic.contentpump.utilities.CommandlineOption;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.ForestInputFormat;
import com.marklogic.mapreduce.Indentation;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.mapreduce.ForestDocument;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;

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
            configConnectionId(options);
            configCopyOptions(options);
            configCommonOutputOptions(options);
            configBatchTxn(options);
            configModule(options);
            
			Option inputFilePath = OptionBuilder
                .withArgName("path")
                .hasArg()
                .withDescription("The file system location for input, as a "
                        + "regular expression")
                .create(INPUT_FILE_PATH);
            inputFilePath.setRequired(true);
            options.addOption(inputFilePath);
            Option inputFilePattern = OptionBuilder
                .withArgName("regex pattern")
                .hasArg()
                .withDescription("Matching regex pattern for files found in "
                    + "the input file path")
                .create(INPUT_FILE_PATTERN);
            options.addOption(inputFilePattern);
            Option aggregateRecordElement = OptionBuilder
                .withArgName("QName")
                .hasArg()
                .withDescription("Element name in which each document is "
                        + "found")
                .create(AGGREGATE_RECORD_ELEMENT);
            options.addOption(aggregateRecordElement);
            Option aggregateRecordNamespace = OptionBuilder
                .withArgName("namespace")
                .hasArg()
                .withDescription("Element namespace in which each document "
                        + "is found")
                .create(AGGREGATE_RECORD_NAMESPACE);
            options.addOption(aggregateRecordNamespace);
            Option aggregateUriId = OptionBuilder
                .withArgName("QName")
                .hasArg()
                .withDescription("Name of the first element or attribute "
                        + "within a record element to be used as document URI."
                        + " If omitted, a sequence id will be generated to "
                        + " form the document URI.")                      
                .create(AGGREGATE_URI_ID);
            options.addOption(aggregateUriId);
            Option inputFileType = OptionBuilder
                .withArgName("type")
                .hasArg()
                .withDescription("Type of input file.  Valid choices are: "
                    + "aggregates, archive, delimited_text, documents, forest,"
                    + "rdf, sequencefile.")
                .create(INPUT_FILE_TYPE);
            options.addOption(inputFileType);
            Option inputCompressed = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether the input data is compressed")
                .create(INPUT_COMPRESSED);
            options.addOption(inputCompressed);
            Option inputCompressionCodec = OptionBuilder
                .withArgName("codec")
                .hasArg()
                .withDescription("Codec used for compression: ZIP, GZIP")
                .create(INPUT_COMPRESSION_CODEC);
            options.addOption(inputCompressionCodec);
            Option documentType = OptionBuilder
                .withArgName("type")
                .hasArg()
                .withDescription("Type of document content. Valid choices: "
                    + "XML, JSON, TEXT, BINARY, and MIXED.  Default type for " 
                    + "document is MIXED, where the type is determined "
                    + "from the MIME type mapping configured in MarkLogic "
                    + "Server.")
                .create(DOCUMENT_TYPE);
            options.addOption(documentType);
            Option delimiter = OptionBuilder
                .withArgName(DELIMITER)
                .hasArg()
                .withDescription("Delimiter for delimited text.")
                .create(DELIMITER);
            options.addOption(delimiter);
            Option delimitedUri = OptionBuilder
                .withArgName("column name")
                .hasArg()
                .withDescription("Delimited uri id for delimited text.")
                .create(DELIMITED_URI_ID);
            options.addOption(delimitedUri);
            Option delimitedRoot = OptionBuilder
                .withArgName("root name")
                .hasArg()
                .withDescription("Root element local name of the XML " +
                        "document constructed from one delimited text record.")
                .create(DELIMITED_ROOT_NAME);
            options.addOption(delimitedRoot);
            Option generateUri = OptionBuilder
                .withArgName("true, false")
                .hasOptionalArg()
                .withDescription("Enables automatic URI generation for " +
                        "delimited text records.")
                .create(DELIMITED_GENERATE_URI);
            options.addOption(generateUri);
            Option namespace = OptionBuilder
                .withArgName(NAMESPACE)
                .hasArg()
                .withDescription("Namespace used for output document.")
                .create(NAMESPACE);
            options.addOption(namespace);
            Option outputLanguage = OptionBuilder
                .withArgName("language")
                .hasArg()
                .withDescription("Language name to associate with output "
                + "documents.  A value of \"en\" indicates that the "
                + "documents are in english.  The default is null, "
                + "which indicates the server default.")
                .create(OUTPUT_LANGUAGE);
            options.addOption(outputLanguage);
            Option outputCleanDir = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to clean dir before output.")
                .create(OUTPUT_CLEANDIR);
            options.addOption(outputCleanDir);
            Option outputDir = OptionBuilder
                .withArgName("directory")
                .hasArg()
                .withDescription("Output directory in MarkLogic.")
                .create(OUTPUT_DIRECTORY);
            options.addOption(outputDir);
            Option outputFilenameCollection = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Filename as collection in output.")
                .create(OUTPUT_FILENAME_AS_COLLECTION);
            options.addOption(outputFilenameCollection);
            Option repairLevel = OptionBuilder
                .withArgName("level")
                .hasArg()
                .withDescription("Whether to repair documents to make it well "
                        + "formed or throw error.")
                .create(XML_REPAIR_LEVEL);
            options.addOption(repairLevel);
            Option seqKeyClass = OptionBuilder
                .withArgName("class name")
                .hasArg()
                .withDescription("Name of class to be used as key to read the "
                        + " input SequenceFile")
                .create(INPUT_SEQUENCEFILE_KEY_CLASS);
            options.addOption(seqKeyClass);
            Option seqValueClass = OptionBuilder
                .withArgName("class name")
                .hasArg()
                .withDescription("Name of class to be used as value to read "
                        + "the input SequenceFile")
                .create(INPUT_SEQUENCEFILE_VALUE_CLASS);
            options.addOption(seqValueClass);
            Option seqValueType = OptionBuilder
                .withArgName("value type")
                .hasArg()
                .withDescription("Type of the value data returned by the above"
                        + " class.  Valid choices are: Text, BytesWritable, "
                        + "MarkLogicDocument and Path.")
                .create(INPUT_SEQUENCEFILE_VALUE_TYPE);
            options.addOption(seqValueType);
            Option allowEmptyMeta = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to allow empty metadata when "
                        + "importing archive")
                .create(ARCHIVE_METADATA_OPTIONAL);
            options.addOption(allowEmptyMeta);
            Option fastLoad = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to use the fast load mode to load "
                        + "content into MarkLogic")
                .create(FAST_LOAD);
            options.addOption(fastLoad);
            Option streaming = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to use streaming to output data to"
                        + " MarkLogic")
                .create(STREAMING);
            options.addOption(streaming);
            Option encoding = OptionBuilder
                .withArgName("encoding")
                .hasOptionalArg()
                .withDescription(
                    "The charset encoding to be used by the MarkLogic when "
                        + "loading documents.  The default is \"UTF-8\".")
                .create(CONTENT_ENCODING);
            options.addOption(encoding);

            Option threadsPerSplit = OptionBuilder.withArgName("count")
                .hasOptionalArg()
                .withDescription("The number of threads per split")
                .create(THREADS_PER_SPLIT);
            options.addOption(threadsPerSplit);

            Option tolerateErrors = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription(
                    "Whether to tolerate insertion errors and make sure all "
                    + "successful inserts are committed")
                .create(TOLERATE_ERRORS);
            options.addOption(tolerateErrors);

            Option rdfMemoryThreshold_opt = OptionBuilder
                    .withArgName("threshold")
                    .hasArg()
                    .withDescription("Maximum size of an RDF document to be processed in memory")
                    .create(RDF_STREAMING_MEMORY_THRESHOLD);
            CommandlineOption rdfMemoryThreshold = new CommandlineOption(rdfMemoryThreshold_opt);
            rdfMemoryThreshold.setHidden(true);
            options.addOption(rdfMemoryThreshold);

            Option rdfTriplesPerDoc_opt = OptionBuilder
                    .withArgName("count")
                    .hasArg()
                    .withDescription("Maximum number of triples per sem:triples document")
                    .create(RDF_TRIPLES_PER_DOCUMENT);
            CommandlineOption rdfTriplesPerDoc = new CommandlineOption(rdfTriplesPerDoc_opt);
            rdfTriplesPerDoc.setHidden(true);
            options.addOption(rdfTriplesPerDoc);

            configPartition(options);
            
            Option splitInput = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription(
                    "Whether to split input files to load into MarkLogic.  "
                     + " Only available for delimited_text.  Default is false.")
                .create(SPLIT_INPUT);
            options.addOption(splitInput);
            
            Option df = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("Comma-separated list of directories")
                .create(DIRECTORY_FILTER);
            options.addOption(df);
            Option cf = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("Comma-separated list of collections")
                .create(COLLECTION_FILTER);
            options.addOption(cf);
            Option tf = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("Comma-separated list of document types")
                .create(TYPE_FILTER);
            options.addOption(tf);
            Option tcf = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("temporal collection name")
                .create(TEMPORAL_COLLECTION);
            options.addOption(tcf);                        
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
                        throws IOException {
            applyConfigOptions(conf, cmdline);
            InputType type = getInputType(cmdline);
            type.applyConfigOptions(conf, cmdline);
            
            // construct a job
            Job job = new Job(conf);
            job.setInputFormatClass(type.getInputFormatClass(cmdline, conf));
            job.setOutputFormatClass(type.getOutputFormatClass(cmdline, conf));

            // set mapper class
            setMapperClass(job, conf, cmdline);
            
            if (cmdline.hasOption(INPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(INPUT_FILE_PATH);
                FileInputFormat.setInputPaths(job, path);
            }
            if (cmdline.hasOption(INPUT_FILE_PATTERN)) {
                FileInputFormat.setInputPathFilter(job,
                                DocumentPathFilter.class);
            }
            
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyCommonOutputConfigOptions(conf, cmdline);
            
            InputType inputType = getInputType(cmdline);   
            ContentType contentType = inputType.getContentType(cmdline);
            conf.set(MarkLogicConstants.CONTENT_TYPE, contentType.name());
            
            if (ContentType.MIXED == contentType) {
                LOG.info("Content type is set to MIXED.  The format of the " +
                        " inserted documents will be determined by the MIME " +
                        " type specification configured on MarkLogic Server.");
            } else {
                LOG.info("Content type: " + contentType.name());
            }
            
            if (Command.isStreaming(cmdline, conf)) {
                conf.setBoolean(MarkLogicConstants.OUTPUT_STREAMING, true);
            }

            if (cmdline.hasOption(ARCHIVE_METADATA_OPTIONAL)) {
                String arg = cmdline.getOptionValue(
                        ARCHIVE_METADATA_OPTIONAL);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(CONF_INPUT_ARCHIVE_METADATA_OPTIONAL,
                                    true);
                } else if (arg.equalsIgnoreCase("false")) {
                    conf.setBoolean(CONF_INPUT_ARCHIVE_METADATA_OPTIONAL,
                                    false);
                } else {
                    throw new IllegalArgumentException(
                        "Unrecognized option argument for "
                        + ARCHIVE_METADATA_OPTIONAL + ": "+ arg);
                }
            }

            if (cmdline.hasOption(INPUT_COMPRESSION_CODEC)) {
                String codec = cmdline.getOptionValue(INPUT_COMPRESSION_CODEC);
                conf.set(CONF_INPUT_COMPRESSION_CODEC, codec.toUpperCase());
            }
            if (cmdline.hasOption(MAX_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE);
                conf.set(CONF_MAX_SPLIT_SIZE1, maxSize);
                conf.set(CONF_MAX_SPLIT_SIZE2, maxSize);
            }
            if (cmdline.hasOption(MIN_SPLIT_SIZE)) {
                String minSize = cmdline.getOptionValue(MIN_SPLIT_SIZE);
                conf.set(CONF_MIN_SPLIT_SIZE1, minSize);
                conf.set(CONF_MIN_SPLIT_SIZE2, minSize);
            }
            if (cmdline.hasOption(AGGREGATE_URI_ID)) {
                String uriId = cmdline.getOptionValue(AGGREGATE_URI_ID);
                conf.set(CONF_AGGREGATE_URI_ID, uriId);
            }
            if (cmdline.hasOption(AGGREGATE_RECORD_ELEMENT)) {
                String recElem = cmdline.getOptionValue(
                        AGGREGATE_RECORD_ELEMENT);
                conf.set(CONF_AGGREGATE_RECORD_ELEMENT, recElem);
            }
            if (cmdline.hasOption(AGGREGATE_RECORD_NAMESPACE)) {
                String recNs = cmdline.getOptionValue(
                        AGGREGATE_RECORD_NAMESPACE);
                conf.set(CONF_AGGREGATE_RECORD_NAMESPACE, recNs);
            }
            if (cmdline.hasOption(DELIMITER)) {
                String delim = cmdline.getOptionValue(DELIMITER);
                conf.set(CONF_DELIMITER, delim);
            }
            if (cmdline.hasOption(DELIMITED_URI_ID)) {
                String delimId = cmdline.getOptionValue(DELIMITED_URI_ID);
                conf.set(CONF_DELIMITED_URI_ID, delimId);
                
                if (cmdline.hasOption(DELIMITED_GENERATE_URI)) {
                    String arg = 
                        cmdline.getOptionValue(DELIMITED_GENERATE_URI);
                    if (arg == null || arg.equalsIgnoreCase("true")) {
                        throw new IllegalArgumentException("The setting for " +
                            DELIMITED_GENERATE_URI + " option conflicts with " 
                            + DELIMITED_URI_ID);
                    }
                }
            }
            if (cmdline.hasOption(DELIMITED_ROOT_NAME)) {
                String delimRoot = cmdline.getOptionValue(DELIMITED_ROOT_NAME);
                conf.set(CONF_DELIMITED_ROOT_NAME, delimRoot);
            }
            if (cmdline.hasOption(DELIMITED_GENERATE_URI)) {
                String arg = 
                    cmdline.getOptionValue(DELIMITED_GENERATE_URI);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(CONF_DELIMITED_GENERATE_URI, true);
                    if (getInputType(cmdline) != InputType.DELIMITED_TEXT) {
                        throw new IllegalArgumentException(
                                DELIMITED_GENERATE_URI + 
                                " is only applicable to " +
                                InputType.DELIMITED_TEXT.name());
                    }
                } else if (!arg.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                            DELIMITED_GENERATE_URI + ": " + arg);
                }
            }
            if (cmdline.hasOption(OUTPUT_FILENAME_AS_COLLECTION)) {
                String arg = cmdline.getOptionValue(
                        OUTPUT_FILENAME_AS_COLLECTION);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(CONF_OUTPUT_FILENAME_AS_COLLECTION, true);
                } else {
                    conf.setBoolean(CONF_OUTPUT_FILENAME_AS_COLLECTION, false);
                }
            }
            if (cmdline.hasOption(OUTPUT_DIRECTORY)) {
                String outDir = cmdline.getOptionValue(OUTPUT_DIRECTORY);
                conf.set(MarkLogicConstants.OUTPUT_DIRECTORY, outDir);
            }
            if (cmdline.hasOption(OUTPUT_CLEANDIR)) {
                String arg = cmdline.getOptionValue(OUTPUT_CLEANDIR);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_CLEAN_DIR, true);
                } else if (arg.equalsIgnoreCase("false")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_CLEAN_DIR, 
                            false);
                } else {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                            OUTPUT_CLEANDIR + ": " + arg);
                }
            }
            String batchSize = cmdline.getOptionValue(BATCH_SIZE);
            if (batchSize != null) {       
                conf.set(MarkLogicConstants.BATCH_SIZE, batchSize);
            }

            String txnSize = cmdline.getOptionValue(TRANSACTION_SIZE);
            if (txnSize != null) {
                conf.set(MarkLogicConstants.TXN_SIZE, txnSize);
            }

            if (cmdline.hasOption(NAMESPACE)) {
                String ns = cmdline.getOptionValue(NAMESPACE);
                conf.set(MarkLogicConstants.OUTPUT_CONTENT_NAMESPACE, ns);
            }
            if (cmdline.hasOption(OUTPUT_LANGUAGE)) {
                String language = cmdline.getOptionValue(OUTPUT_LANGUAGE);
                conf.set(MarkLogicConstants.OUTPUT_CONTENT_LANGUAGE, language);
            }
            if (cmdline.hasOption(INPUT_FILE_PATTERN)) {
                if (inputType == InputType.FOREST) {
                    LOG.warn("The setting for " + INPUT_FILE_PATTERN + 
                            " is ignored for input type " + inputType.name());
                } else {
                    String pattern = 
                            cmdline.getOptionValue(INPUT_FILE_PATTERN);
                    conf.set(CONF_INPUT_FILE_PATTERN, pattern);
                }
            }
            if (cmdline.hasOption(USERNAME)) {
                String username = cmdline.getOptionValue(USERNAME);
                conf.set(MarkLogicConstants.OUTPUT_USERNAME, username);
            }
            if (cmdline.hasOption(PASSWORD)) {
                String password = cmdline.getOptionValue(PASSWORD);
                conf.set(MarkLogicConstants.OUTPUT_PASSWORD, password);
            }
            if (cmdline.hasOption(HOST)) {
                String host = cmdline.getOptionValue(HOST);
                conf.set(MarkLogicConstants.OUTPUT_HOST, host);
            }
            if (cmdline.hasOption(PORT)) {
                String port = cmdline.getOptionValue(PORT);
                conf.set(MarkLogicConstants.OUTPUT_PORT, port);
            }
            if (cmdline.hasOption(DATABASE)) {
                String db = cmdline.getOptionValue(DATABASE);
                conf.set(MarkLogicConstants.OUTPUT_DATABASE_NAME, db);
            }
            if (cmdline.hasOption(TEMPORAL_COLLECTION)) {
                String tempColl = cmdline.getOptionValue(TEMPORAL_COLLECTION);
                conf.set(MarkLogicConstants.TEMPORAL_COLLECTION, tempColl);
            }

            String repairLevel = cmdline.getOptionValue(XML_REPAIR_LEVEL,
                    MarkLogicConstants.DEFAULT_OUTPUT_XML_REPAIR_LEVEL);
            conf.set(MarkLogicConstants.OUTPUT_XML_REPAIR_LEVEL, 
                    repairLevel.toUpperCase());
            if (cmdline.hasOption(INPUT_SEQUENCEFILE_KEY_CLASS)) {
                String keyClass = cmdline.getOptionValue(
                        INPUT_SEQUENCEFILE_KEY_CLASS);
                conf.set(CONF_INPUT_SEQUENCEFILE_KEY_CLASS, keyClass);
            }
            if (cmdline.hasOption(INPUT_SEQUENCEFILE_VALUE_CLASS)) {
                String valueClass = cmdline.getOptionValue(
                        INPUT_SEQUENCEFILE_VALUE_CLASS);
                conf.set(CONF_INPUT_SEQUENCEFILE_VALUE_CLASS, valueClass);
            }
            if (cmdline.hasOption(INPUT_SEQUENCEFILE_VALUE_TYPE)) {
                String valueType = cmdline.getOptionValue(
                                INPUT_SEQUENCEFILE_VALUE_TYPE,
                                DEFAULT_SEQUENCEFILE_VALUE_TYPE);
                conf.set(CONF_INPUT_SEQUENCEFILE_VALUE_TYPE, 
                        valueType.toUpperCase());
                if (valueType.equalsIgnoreCase(
                        SequenceFileValueType.BYTESWRITABLE.toString())) {
                    conf.set(MarkLogicConstants.CONTENT_TYPE,
                                    ContentType.BINARY.toString());
                }
            } else if (conf.get(CONF_INPUT_SEQUENCEFILE_VALUE_TYPE) == null) {
                conf.set(CONF_INPUT_SEQUENCEFILE_VALUE_TYPE,
                                DEFAULT_SEQUENCEFILE_VALUE_TYPE);
            }
            if (cmdline.hasOption(INPUT_FILE_TYPE)) {
                String fileType = cmdline.getOptionValue(INPUT_FILE_TYPE);
                if (fileType.equalsIgnoreCase(InputType.ARCHIVE.toString())) {
                    conf.set(MarkLogicConstants.CONTENT_TYPE,
                                    ContentType.UNKNOWN.toString());
                }
            }
            if (cmdline.hasOption(FAST_LOAD)) {
                String arg = cmdline.getOptionValue(FAST_LOAD);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, true);
                    LOG.info("Option fastload is specified.Please make sure "+
                             "that all conditions required to run in fastload "+
                             "mode are satisfied to avoid XDMP-DBDUPURI "+
                             "errors.");
                } else if (arg.equalsIgnoreCase("false")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, false);
                } else {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + FAST_LOAD
                                    + ": " + arg);
                }
            }
            if (cmdline.hasOption(CONTENT_ENCODING)) {
                String arg = cmdline.getOptionValue(CONTENT_ENCODING).toUpperCase();
                if("SYSTEM".equals(arg)) {
                    arg = Charset.defaultCharset().name();
                } else if (!Charset.isSupported(arg)) {
                    throw new IllegalArgumentException( arg
                        + " encoding is not supported");
                }
                conf.set(MarkLogicConstants.OUTPUT_CONTENT_ENCODING, arg);
            }
            if (cmdline.hasOption(THREADS_PER_SPLIT)) {
                String arg = cmdline.getOptionValue(THREADS_PER_SPLIT);
                int threadCnt = Integer.parseInt(arg);
                if (threadCnt > 1 && isStreaming(cmdline, conf)) {
                    LOG.warn("The setting for " + THREADS_PER_SPLIT + 
                            " is ignored because streaming is enabled.");
                } else if (threadCnt < inputType.getMinThreads()) {
                    throw new IllegalArgumentException("Cannot set " + 
                            THREADS_PER_SPLIT + 
                            " to a value less than the minimum required " +
                            " threads (" + inputType.getMinThreads() +
                            ")for the job.");
                } else {
                    conf.set(CONF_THREADS_PER_SPLIT, arg);
                }
            }
            if (cmdline.hasOption(THREAD_COUNT)) {
                String arg = cmdline.getOptionValue(THREAD_COUNT);
                int threadCnt = Integer.parseInt(arg);
                if (threadCnt < inputType.getMinThreads()) {
                    throw new IllegalArgumentException("Cannot set " + 
                            THREAD_COUNT + 
                            " to a value less than the minimum required " +
                            " threads (" + inputType.getMinThreads() +
                            ")for the job.");
                }
            }
            if (cmdline.hasOption(TEMPORAL_COLLECTION)) {
                String fileType = cmdline.getOptionValue(INPUT_FILE_TYPE);
                if (fileType != null && 
                    fileType.equalsIgnoreCase(InputType.RDF.toString())) {
                    throw new IllegalArgumentException(
                        "Cannot ingest RDF into temporal collection");
                }
            	if (contentType != null && ContentType.BINARY == contentType) {
                    throw new IllegalArgumentException(
                        "Cannot ingest BINARY into temporal collection");
                }
            }
            if (cmdline.hasOption(TOLERATE_ERRORS)) {
                String arg = cmdline.getOptionValue(TOLERATE_ERRORS);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_TOLERATE_ERRORS, true);
                } else if (arg.equalsIgnoreCase("false")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_TOLERATE_ERRORS, false);
                } else {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + TOLERATE_ERRORS
                                    + ": " + arg);
                }
            }
            
            applyPartitionConfigOptions(conf, cmdline);
            
            applyModuleConfigOptions(conf, cmdline, batchSize);
            
            if (cmdline.hasOption(SPLIT_INPUT)) {
                String arg = cmdline.getOptionValue(SPLIT_INPUT);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    if (isInputCompressed(cmdline)) {
                        LOG.warn(INPUT_COMPRESSED + " disables " + SPLIT_INPUT);
                        conf.setBoolean(CONF_SPLIT_INPUT, false);
                    }
                    if (inputType != InputType.DELIMITED_TEXT) {
                        throw new IllegalArgumentException("The setting for " +
                            SPLIT_INPUT + " option is not supported for " + 
                            inputType);
                    }
                    conf.setBoolean(CONF_SPLIT_INPUT, true);
                } else if (arg.equalsIgnoreCase("false")) {
                    conf.setBoolean(CONF_SPLIT_INPUT, false);
                } else {
                    throw new IllegalArgumentException(
                        "Unrecognized option argument for " + SPLIT_INPUT
                            + ": " + arg);
                }
            }
            if (cmdline.hasOption(COLLECTION_FILTER)) {
                if (inputType == InputType.FOREST) {
                    String colFilter = 
                            cmdline.getOptionValue(COLLECTION_FILTER);
                    conf.set(CONF_COLLECTION_FILTER, colFilter);
                } else {
                    LOG.warn("The setting for " + COLLECTION_FILTER + 
                            " is not applicable for " + inputType); 
                }
            }
            if (cmdline.hasOption(DIRECTORY_FILTER)) {
                if (inputType == InputType.FOREST) {
                    String dirFilter = 
                            cmdline.getOptionValue(DIRECTORY_FILTER);
                    conf.set(CONF_DIRECTORY_FILTER, dirFilter);
                } else {
                    LOG.warn("The setting for " + DIRECTORY_FILTER + 
                            " is not applicable for " + inputType); 
                }
            }
            if (cmdline.hasOption(TYPE_FILTER)) {
                if (inputType == InputType.FOREST) {
                    String typeFilter = 
                            cmdline.getOptionValue(TYPE_FILTER);
                    conf.set(MarkLogicConstants.TYPE_FILTER, typeFilter);
                } else {
                    LOG.warn("The setting for " + TYPE_FILTER + 
                            " is not applicable for " + inputType); 
                }
            }
        }

		@Override
		public void setMapperClass(Job job, Configuration conf,
				CommandLine cmdline) {
			String inputTypeOption = cmdline.getOptionValue(INPUT_FILE_TYPE,
                    INPUT_FILE_TYPE_DEFAULT);
            InputType type = InputType.forName(inputTypeOption);
            
            int minThreads = type.getMinThreads();
            if (minThreads > 1) {
                conf.setInt(CONF_MIN_THREADS, minThreads);
            }
            int threadCnt = conf.getInt(CONF_THREADS_PER_SPLIT, 1);
            threadCnt = Math.max(threadCnt, minThreads);
 
			Class<? extends BaseMapper<?, ?, ?, ?>> internalMapperClass =
					type.getMapperClass(cmdline, conf);
            if (threadCnt > 1 && !isStreaming(cmdline, conf)) {
                job.setMapperClass(MultithreadedMapper.class);
                MultithreadedMapper.setMapperClass(job.getConfiguration(),
                    internalMapperClass);
                MultithreadedMapper.setNumberOfThreads(job, threadCnt);
            } else {
                // thread_count_per_split is not greater than 1
                job.setMapperClass(internalMapperClass);
            }
		}

		@SuppressWarnings("unchecked")
        @Override
		public Class<? extends Mapper<?,?,?,?>> getRuntimeMapperClass(Job job,
				Class<? extends Mapper<?,?,?,?>> mapper, int threadCnt, 
				int availableThreads) {
			if (threadCnt == 0 && availableThreads > 1 &&
			    !job.getConfiguration().getBoolean(
			            MarkLogicConstants.OUTPUT_STREAMING, false)) {
                Class<? extends Mapper<?, ?, ?, ?>> mapperClass = 
                	(Class<? extends Mapper<?, ?, ?, ?>>) (Class) MultithreadedMapper.class;
                MultithreadedMapper.setMapperClass(job.getConfiguration(),
                    (Class<? extends BaseMapper<?, ?, ?, ?>>) mapper);
                return mapperClass;
            } else {
                return mapper;
            }
		}
    },
    EXPORT {
        @Override
        public void configOptions(Options options) {
            configCommonOptions(options);
            configConnectionId(options);
            configCopyOptions(options);
            configFilteringOptions(options);

            Option outputType = OptionBuilder
                .withArgName("type")
                .hasArg()
                .withDescription("export output type")
                .create(OUTPUT_TYPE);
            options.addOption(outputType);
            Option outputFilePath = OptionBuilder
                .withArgName("path")
                .hasArg()
                .withDescription("export output file path")
                .create(OUTPUT_FILE_PATH);
            outputFilePath.setRequired(true);                
            options.addOption(outputFilePath);
            Option exportCompress = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to compress the output document")
                .create(OUTPUT_COMPRESS);
            options.addOption(exportCompress);
            Option exportIndented = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to format data with indentation")
                .create(OUTPUT_INDENTED);
            options.addOption(exportIndented);
            Option snapshot = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to use a consistent timestamp to " +
                        "fetch data from the source database")
                .create(SNAPSHOT);
            options.addOption(snapshot);
            Option encoding = OptionBuilder
                .withArgName("encoding")
                .hasOptionalArg()
                .withDescription(
                    "The charset encoding to be used by the MarkLogic when "
                        + "exporting documents").create(CONTENT_ENCODING);
            options.addOption(encoding);
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
                        throws IOException {
            applyConfigOptions(conf, cmdline);

            String type = conf.get(CONF_OUTPUT_TYPE, DEFAULT_OUTPUT_TYPE);
            ExportOutputType outputType = ExportOutputType.valueOf(
                            type.toUpperCase());
            if (outputType.equals(ExportOutputType.DOCUMENT)) {  
                conf.set(MarkLogicConstants.INPUT_VALUE_CLASS,
                                DatabaseDocument.class.getCanonicalName());
            }
            
            if (cmdline.hasOption(SNAPSHOT)) {
                String arg = cmdline.getOptionValue(SNAPSHOT);
                if (arg == null || arg.equalsIgnoreCase("true")){
                    setQueryTimestamp(conf);
                } else if (!arg.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + SNAPSHOT
                            + ": " + arg);
                }
            }
            
            // construct a job
            Job job = new Job(conf);
            job.setJarByClass(this.getClass());
            job.setInputFormatClass(outputType.getInputFormatClass());

            setMapperClass(job, conf, cmdline);
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(MarkLogicDocument.class);
            job.setOutputFormatClass(
                            outputType.getOutputFormatClass(cmdline));
            job.setOutputKeyClass(DocumentURI.class);
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyFilteringConfigOptions(conf, cmdline);

            if (cmdline.hasOption(OUTPUT_TYPE)) {
                String outputType = cmdline.getOptionValue(OUTPUT_TYPE);
                conf.set(CONF_OUTPUT_TYPE, outputType);
            }
            if (cmdline.hasOption(OUTPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(OUTPUT_FILE_PATH);
                String wkdir = conf.get("mapred.working.dir");
                if (wkdir != null) {
                    path = new Path(wkdir, path).toString();
                }
                conf.set(CONF_OUTPUT_FILEPATH, path);
            }
            if (cmdline.hasOption(OUTPUT_INDENTED)) {
                String isIndented = cmdline.getOptionValue(OUTPUT_INDENTED);
                // check value validity
                Indentation indent = Indentation.forName(isIndented);
                conf.set(MarkLogicConstants.INDENTED, indent.name());
            }
            if (cmdline.hasOption(HOST)) {
                String host = cmdline.getOptionValue(HOST);
                conf.set(MarkLogicConstants.INPUT_HOST, host);
            }
            if (cmdline.hasOption(PORT)) {
                String port = cmdline.getOptionValue(PORT);
                conf.set(MarkLogicConstants.INPUT_PORT, port);
            }
            if (cmdline.hasOption(USERNAME)) {
                String user = cmdline.getOptionValue(USERNAME);
                conf.set(MarkLogicConstants.INPUT_USERNAME, user);
            }
            if (cmdline.hasOption(PASSWORD)) {
                String pswd = cmdline.getOptionValue(PASSWORD);
                conf.set(MarkLogicConstants.INPUT_PASSWORD, pswd);
            }
            if (cmdline.hasOption(DATABASE)) {
                String db = cmdline.getOptionValue(DATABASE);
                conf.set(MarkLogicConstants.INPUT_DATABASE_NAME, db);
            }
            if (cmdline.hasOption(MAX_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE);
                conf.set(MarkLogicConstants.MAX_SPLIT_SIZE, maxSize);
            }
            if (cmdline.hasOption(CONTENT_ENCODING)) {
                String arg = cmdline.getOptionValue(CONTENT_ENCODING).toUpperCase();
                if("SYSTEM".equals(arg)) {
                    arg = Charset.defaultCharset().name();
                } else if (!Charset.isSupported(arg)) {
                    throw new IllegalArgumentException(arg
                        + " encoding is not supported");
                }
                conf.set(MarkLogicConstants.OUTPUT_CONTENT_ENCODING, arg);
            }
        }

		@Override
		public void setMapperClass(Job job, Configuration conf,
				CommandLine cmdline) {
			job.setMapperClass(DocumentMapper.class);			
		}

		@Override
		public Class<? extends Mapper<?,?,?,?>> 
	    getRuntimeMapperClass(Job job, 
				Class<? extends Mapper<?,?,?,?>> mapper, int threadCnt, 
				int availableThreads) {
			return mapper;
		}
    },
    COPY {
        @Override
        public void configOptions(Options options) {
            configCommonOptions(options);
            configCopyOptions(options);
            configCommonOutputOptions(options);
            configFilteringOptions(options);
            configBatchTxn(options);
            configModule(options);
            
            Option inputUsername = OptionBuilder
                .withArgName("username")
                .hasArg()
                .withDescription("User name of the input MarkLogic Server")
                .create(INPUT_USERNAME);
            inputUsername.setRequired(true);
            options.addOption(inputUsername);
            Option inputPassword = OptionBuilder
                .withArgName("password")
                .hasArg()
                .withDescription("Password of the input MarkLogic Server")
                .create(INPUT_PASSWORD);
            inputPassword.setRequired(true);
            options.addOption(inputPassword);
            Option inputHost = OptionBuilder
                .withArgName("host")
                .hasArg()
                .withDescription("Host of the input MarkLogic Server")
                .create(INPUT_HOST);
            inputHost.setRequired(true);
            options.addOption(inputHost);
            Option inputPort = OptionBuilder
                .withArgName("port")
                .hasArg()
                .withDescription("Port of the input MarkLogic Server")
                .create(INPUT_PORT);
            options.addOption(inputPort);
            Option inputDB = OptionBuilder
                .withArgName("database")
                .hasArg()
                .withDescription("Database of the input MarkLogic Server")
                .create(INPUT_DATABASE);
            options.addOption(inputDB);
            Option outputUsername = OptionBuilder
                .withArgName("username")
                .hasArg()
                .withDescription("User Name of the output MarkLogic Server")
                .create(OUTPUT_USERNAME);
            outputUsername.setRequired(true);
            options.addOption(outputUsername);
            Option outputPassword = OptionBuilder
                .withArgName("password")
                .hasArg()
                .withDescription("Password of the output MarkLogic Server")
                .create(OUTPUT_PASSWORD);
            outputPassword.setRequired(true);
            options.addOption(outputPassword);
            Option outputHost = OptionBuilder
                .withArgName("host")
                .hasArg()
                .withDescription("Host of the output MarkLogic Server")
                .create(OUTPUT_HOST);
            outputHost.setRequired(true);
            options.addOption(outputHost);
            Option outputPort = OptionBuilder
                 .withArgName("port")
                 .hasArg()
                 .withDescription("Port of the output MarkLogic Server")
                 .create(OUTPUT_PORT);
            options.addOption(outputPort);
            Option outputDB = OptionBuilder
                .withArgName("database")
                .hasArg()
                .withDescription("Database of the output MarkLogic Server")
                .create(OUTPUT_DATABASE);
            options.addOption(outputDB);
            Option tcf = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("temporal collection name, used only" +
                    " for temporal documents")
                .create(TEMPORAL_COLLECTION);
            options.addOption(tcf); 
            Option fastLoad = OptionBuilder
                 .withArgName("true,false")
                 .hasOptionalArg()
                 .withDescription("Whether to use the fast load mode to load "
                         + "content into MarkLogic")
                 .create(FAST_LOAD);
            options.addOption(fastLoad);
            Option outputDir = OptionBuilder
                .withArgName("directory")
                .hasArg()
                .withDescription("Output directory in MarkLogic.")
                .create(OUTPUT_DIRECTORY);
            options.addOption(outputDir);
            Option tolerateErrors = OptionBuilder
                .withArgName("tolerate errors")
                .hasOptionalArg()
                .withDescription(
                    "Whether to tolerate insertion errors and make sure all "
                    + "successful inserts are committed")
                .create(TOLERATE_ERRORS);
            options.addOption(tolerateErrors);
            
            configPartition(options);
            
            Option snapshot = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to use a consistent timestamp to " +
                    "fetch data from the source database")
                .create(SNAPSHOT);
            options.addOption(snapshot);
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
                        throws IOException {
            applyConfigOptions(conf, cmdline);
            
            if (cmdline.hasOption(SNAPSHOT)) {
                String arg = cmdline.getOptionValue(SNAPSHOT);
                if (arg == null || arg.equalsIgnoreCase("true")){
                    setQueryTimestamp(conf);
                } else if (!arg.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + SNAPSHOT
                            + ": " + arg);
                } 
            }
            Job job = new Job(conf);
            job.setJarByClass(this.getClass());
            job.setInputFormatClass(DatabaseContentInputFormat.class);
            job.setMapperClass(DocumentMapper.class);
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(MarkLogicDocument.class);
            if(cmdline.hasOption(TRANSFORM_MODULE)) {
                job.setOutputFormatClass(DatabaseTransformOutputFormat.class);
            } else {
                job.setOutputFormatClass(DatabaseContentOutputFormat.class);
            }
            job.setOutputKeyClass(DocumentURI.class);
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyFilteringConfigOptions(conf, cmdline);
            applyCommonOutputConfigOptions(conf, cmdline);

            if (cmdline.hasOption(OUTPUT_USERNAME)) {
                String username = cmdline.getOptionValue(OUTPUT_USERNAME);
                conf.set(MarkLogicConstants.OUTPUT_USERNAME, username);
            }
            if (cmdline.hasOption(OUTPUT_PASSWORD)) {
                String password = cmdline.getOptionValue(OUTPUT_PASSWORD);
                conf.set(MarkLogicConstants.OUTPUT_PASSWORD, password);
            }
            if (cmdline.hasOption(OUTPUT_HOST)) {
                String host = cmdline.getOptionValue(OUTPUT_HOST);
                conf.set(MarkLogicConstants.OUTPUT_HOST, host);
            }
            if (cmdline.hasOption(OUTPUT_PORT)) {
                String port = cmdline.getOptionValue(OUTPUT_PORT);
                conf.set(MarkLogicConstants.OUTPUT_PORT, port);
            }
            if (cmdline.hasOption(OUTPUT_DATABASE)) {
                String db = cmdline.getOptionValue(OUTPUT_DATABASE);
                conf.set(MarkLogicConstants.OUTPUT_DATABASE_NAME, db);
            }

            if (cmdline.hasOption(INPUT_USERNAME)) {
                String username = cmdline.getOptionValue(INPUT_USERNAME);
                conf.set(MarkLogicConstants.INPUT_USERNAME, username);
            }
            if (cmdline.hasOption(INPUT_PASSWORD)) {
                String password = cmdline.getOptionValue(INPUT_PASSWORD);
                conf.set(MarkLogicConstants.INPUT_PASSWORD, password);
            }
            if (cmdline.hasOption(INPUT_HOST)) {
                String host = cmdline.getOptionValue(INPUT_HOST);
                conf.set(MarkLogicConstants.INPUT_HOST, host);
            }
            if (cmdline.hasOption(INPUT_PORT)) {
                String port = cmdline.getOptionValue(INPUT_PORT);
                conf.set(MarkLogicConstants.INPUT_PORT, port);
            }
            if (cmdline.hasOption(INPUT_DATABASE)) {
                String db = cmdline.getOptionValue(INPUT_DATABASE);
                conf.set(MarkLogicConstants.INPUT_DATABASE_NAME, db);
            }
            if (cmdline.hasOption(TEMPORAL_COLLECTION)) {
                String tempColl = cmdline.getOptionValue(TEMPORAL_COLLECTION);
                conf.set(MarkLogicConstants.TEMPORAL_COLLECTION, tempColl);
            }
            if (cmdline.hasOption(MAX_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE);
                conf.set(MarkLogicConstants.MAX_SPLIT_SIZE, maxSize);
            }
            if (cmdline.hasOption(FAST_LOAD)) {
                String arg = cmdline.getOptionValue(FAST_LOAD);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, true);
                } else {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, 
                            false);
                }
            }
            if (cmdline.hasOption(OUTPUT_DIRECTORY)) {
                String outDir = cmdline.getOptionValue(OUTPUT_DIRECTORY);
                conf.set(MarkLogicConstants.OUTPUT_DIRECTORY, outDir);
            }
            String batchSize = cmdline.getOptionValue(BATCH_SIZE);
            if (batchSize != null) {       
                conf.set(MarkLogicConstants.BATCH_SIZE, batchSize);
            }

            String txnSize = cmdline.getOptionValue(TRANSACTION_SIZE);
            if (txnSize != null) {
                conf.set(MarkLogicConstants.TXN_SIZE, txnSize);
            }
            
            if (cmdline.hasOption(TOLERATE_ERRORS)) {
                String arg = cmdline.getOptionValue(TOLERATE_ERRORS);
                conf.set(MarkLogicConstants.OUTPUT_TOLERATE_ERRORS, arg);
            }
            if (cmdline.hasOption(TEMPORAL_COLLECTION)) {
                InputType inputType = getInputType(cmdline);   
                String fileType = cmdline.getOptionValue(INPUT_FILE_TYPE);
                ContentType contentType = inputType.getContentType(cmdline);
                if (fileType != null && 
                    fileType.equalsIgnoreCase(InputType.RDF.toString())) {
                    throw new IllegalArgumentException(
                        "Cannot ingest RDF into temporal collection");
                }
                if (contentType != null && ContentType.BINARY == contentType) {
                    throw new IllegalArgumentException(
    		        "Cannot ingest BINARY into temporal collection");
                }
            }
            
            applyPartitionConfigOptions(conf, cmdline);
            
            applyModuleConfigOptions(conf, cmdline, batchSize);
        }

	@Override
	public void setMapperClass(Job job, Configuration conf,
                                   CommandLine cmdline) {
	    job.setMapperClass(DocumentMapper.class);			
        }

	@Override
	public Class<? extends Mapper<?,?,?,?>> getRuntimeMapperClass(Job job, 
            Class<? extends Mapper<?,?,?,?>> mapper, int threadCnt, 
            int availableThreads) {
                return mapper;
        }
    },
    EXTRACT {
        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {  
            if (cmdline.hasOption(OUTPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(OUTPUT_FILE_PATH);
                String wkdir = conf.get("mapred.working.dir");
                if (wkdir != null) {
                    path = new Path(wkdir, path).toString();
                }
                conf.set(CONF_OUTPUT_FILEPATH, path);
            }
            if (cmdline.hasOption(MIN_SPLIT_SIZE)) {
                String minSize = cmdline.getOptionValue(MIN_SPLIT_SIZE);
                conf.set(CONF_MIN_SPLIT_SIZE1, minSize);
                conf.set(CONF_MIN_SPLIT_SIZE2, minSize);
            }
            if (cmdline.hasOption(MAX_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE);
                conf.set(CONF_MAX_SPLIT_SIZE1, maxSize);
                conf.set(CONF_MAX_SPLIT_SIZE2, maxSize);
            }
            if (cmdline.hasOption(COLLECTION_FILTER)) {
                String colFilter = cmdline.getOptionValue(COLLECTION_FILTER);
                conf.set(CONF_COLLECTION_FILTER, colFilter);
            }
            if (cmdline.hasOption(DIRECTORY_FILTER)) {
                String dirFilter = cmdline.getOptionValue(DIRECTORY_FILTER);
                conf.set(CONF_DIRECTORY_FILTER, dirFilter);
            }
            if (cmdline.hasOption(TYPE_FILTER)) {
                String typeFilter = cmdline.getOptionValue(TYPE_FILTER);
                conf.set(MarkLogicConstants.TYPE_FILTER, typeFilter);
            }
        }

        @Override
        public void configOptions(Options options) {
            configCommonOptions(options); 
            Option inputFilePath = OptionBuilder
                .withArgName("path")
                .hasArg()
                .withDescription("The file system location for input, as a "
                    + "regular expression")
                .create(INPUT_FILE_PATH);
            inputFilePath.setRequired(true);
            options.addOption(inputFilePath);
            Option outputFilePath = OptionBuilder
                .withArgName("path")
                .hasArg()
                .withDescription("export output file path")
                .create(OUTPUT_FILE_PATH);
            outputFilePath.setRequired(true);                
            options.addOption(outputFilePath);
            Option exportCompress = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to compress the output document")
                .create(OUTPUT_COMPRESS);
            options.addOption(exportCompress);   
            Option df = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("Comma-separated list of directories")
                .create(DIRECTORY_FILTER);
            options.addOption(df);
            Option cf = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("Comma-separated list of collections")
                .create(COLLECTION_FILTER);
            options.addOption(cf);
            Option tf = OptionBuilder
                .withArgName("String")
                .hasArg()
                .withDescription("Comma-separated list of document types")
               .create(TYPE_FILTER);
            options.addOption(tf);
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
                throws IOException {
            applyConfigOptions(conf, cmdline);
            
            // construct a job
            Job job = new Job(conf);
            job.setInputFormatClass(ForestInputFormat.class);
            Class<? extends OutputFormat> outputFormatClass = 
                    Command.isOutputCompressed(cmdline) ?
                        ArchiveOutputFormat.class : 
                        SingleDocumentOutputFormat.class;
            job.setOutputFormatClass(outputFormatClass);

            setMapperClass(job, conf, cmdline);
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(ForestDocument.class);
            
            if (cmdline.hasOption(INPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(INPUT_FILE_PATH);
                FileInputFormat.setInputPaths(job, path);
            }
            return job;
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getRuntimeMapperClass(
                Job job, Class<? extends Mapper<?, ?, ?, ?>> mapper,
                int threadCnt, int availableThreads) {
            return mapper;
        }

        @Override
        public void setMapperClass(Job job, Configuration conf,
                CommandLine cmdline) {
            job.setMapperClass(DocumentMapper.class);           
        }
    };

    public static final Log LOG = LogFactory.getLog(LocalJobRunner.class);

    public static Command forName(String cmd) {
        if (cmd.equalsIgnoreCase(IMPORT.name())) {
            return IMPORT;
        } else if (cmd.equalsIgnoreCase(EXPORT.name())) {
            return EXPORT;
        } else if (cmd.equalsIgnoreCase(COPY.name())) {
            return COPY;
        } else if (cmd.equalsIgnoreCase(EXTRACT.name())) {
            return EXTRACT;
        } else {
            throw new IllegalArgumentException("Unknown command: " + cmd);
        }
    }
    
    protected static boolean isInputCompressed(CommandLine cmdline) {
        if (cmdline.hasOption(INPUT_COMPRESSED)) {
            String isCompress = cmdline.getOptionValue(INPUT_COMPRESSED);
            if (isCompress == null || isCompress.equalsIgnoreCase("true")) {
                return true;
            }
        }
        return false;
    }
    
    protected static boolean isOutputCompressed(CommandLine cmdline) {
        if (cmdline.hasOption(OUTPUT_COMPRESS)) {
            String isCompress = cmdline.getOptionValue(OUTPUT_COMPRESS);
            if (isCompress == null || isCompress.equalsIgnoreCase("true")) {
                return true;
            }
        }
        return false;
    }
    
    protected static boolean isStreaming(CommandLine cmdline, 
            Configuration conf) {
        if (conf.get(MarkLogicConstants.OUTPUT_STREAMING) != null) {
            return conf.getBoolean(MarkLogicConstants.OUTPUT_STREAMING, false);
        }
        String arg = null;
        if (cmdline.hasOption(STREAMING)) {
            arg = cmdline.getOptionValue(STREAMING);
            if (arg == null || arg.equalsIgnoreCase("true")) {
                InputType inputType = getInputType(cmdline);
                if (inputType != InputType.DOCUMENTS) {
                    LOG.warn("Streaming option is not applicable to input " +
                            "type " + inputType);
                }
                conf.setBoolean(MarkLogicConstants.OUTPUT_STREAMING, true);
                return true;
            } 
        } 
        conf.setBoolean(MarkLogicConstants.OUTPUT_STREAMING, false);
        return false;
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
     * @param conf
     *            Hadoop configuration
     * @param options
     *            options
     * @return a Hadoop job
     * @throws Exception
     */
    public abstract Job createJob(Configuration conf, CommandLine cmdline)
                    throws IOException;

    /**
     * Apply config options set from the command-line to the configuration.
     * 
     * @param conf
     *            configuration
     * @param cmdline
     *            command line options
     */
    public abstract void applyConfigOptions(Configuration conf,
                    CommandLine cmdline);
    
    /**
     * Set Mapper class for a job.  If the minimum threads required is more
     * than 1 and threads_per_split is not set, also set the minimum threads in
     * the configuration for the job scheduler.
     * 
     * @param job the Hadoop job 
     * @param conf Hadoop configuration
     * @param CommandLine command line
     */
    public abstract void setMapperClass(Job job, Configuration conf, 
    		CommandLine cmdline);
    
    public abstract Class<? extends Mapper<?,?,?,?>> getRuntimeMapperClass(
            Job job, Class<? extends Mapper<?,?,?,?>> mapper, int threadCnt, 
            int availableThreads);

    static void setQueryTimestamp(Configuration conf) 
    throws IOException {
        try {
            ContentSource cs = InternalUtilities.getInputContentSource(conf);
            Session session = cs.newSession();
            conf.set(MarkLogicConstants.INPUT_QUERY_TIMESTAMP, 
                    session.getCurrentServerPointInTime().toString());
        } catch (Exception ex) {
            throw new IOException("Error getting query timestamp", ex);
        }     
    }
    
    static void configCommonOptions(Options options) {
        Option mode = OptionBuilder
            .withArgName(MODE)
            .hasArg()
            .withDescription("Whether to run in local or distributed mode.")
            .create(MODE);
        options.addOption(mode);
        Option hadoopConfDir = OptionBuilder
            .withArgName("directory")
            .hasArg()
            .withDescription("Override $HADOOP_CONF_DIR")
            .create(HADOOP_CONF_DIR);
        options.addOption(hadoopConfDir);
        Option threadCount = OptionBuilder
            .withArgName("count")
            .hasArg()
            .withDescription("Number of threads")
            .create(THREAD_COUNT);
        options.addOption(threadCount);
        Option maxSplitSize = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription("Maximum number of MarkLogic documents per each "
                    + "input split in export or copy, or maximum number of " 
                    + "bytes in file per each split in import")
            .create(MAX_SPLIT_SIZE);
        options.addOption(maxSplitSize);
        Option minSplitSize = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription("Minimum number of bytes in file per each split "
                    + "in import")
            .create(MIN_SPLIT_SIZE);
        options.addOption(minSplitSize);
    }

    static void configCommonOutputOptions(Options options) {
        Option outputUriReplace = OptionBuilder
            .withArgName("list")
            .hasArg()
            .withDescription("Comma separated list of regex pattern and "
                    + "string pairs, 1st to match a uri segment, 2nd the "
                    + "string to replace with, with the 2nd one in ''")
            .create(OUTPUT_URI_REPLACE);
        options.addOption(outputUriReplace);
        Option outputUriPrefix = OptionBuilder
            .withArgName("prefix")
            .hasArg()
            .withDescription("String to prepend to all document URIs")
            .create(OUTPUT_URI_PREFIX);
        options.addOption(outputUriPrefix);
        Option outputUriSuffix = OptionBuilder
            .withArgName("suffix")
            .hasArg()
            .withDescription("String to append to all document URIs")
            .create(OUTPUT_URI_SUFFIX);
        options.addOption(outputUriSuffix);

        Option outputCollections = OptionBuilder
            .withArgName("collections")
            .hasArg()
            .withDescription("Comma separated list of collection to be applied"
                    + " to output documents")
            .create(OUTPUT_COLLECTIONS);
        options.addOption(outputCollections);
        Option outputPermissions = OptionBuilder
            .withArgName("permissions")
            .hasArg()
            .withDescription("Comma separated list of user-privilege pairs to "
                    + "be applied to output documents")
            .create(OUTPUT_PERMISSIONS);
        options.addOption(outputPermissions);
        Option outputQuantity = OptionBuilder
            .withArgName("quality")
            .hasArg()
            .withDescription("Quality to be applied to output documents")
            .create(OUTPUT_QUALITY);
        options.addOption(outputQuantity);
    }

    static void configConnectionId(Options options) {
        Option username = OptionBuilder
            .withArgName(USERNAME)
            .hasArg()
            .withDescription("User name of MarkLogic Server")
            .create(USERNAME);
        username.setRequired(true);
        options.addOption(username);
        Option password = OptionBuilder
            .withArgName(PASSWORD)
            .hasArg()
            .withDescription("Password of MarkLogic Server")
            .create(PASSWORD);
        password.setRequired(true);
        options.addOption(password);
        Option host = OptionBuilder
            .withArgName(HOST)
            .hasArg()
            .withDescription("Host of MarkLogic Server")
            .create(HOST);
        host.setRequired(true);
        options.addOption(host);
        Option port = OptionBuilder
            .withArgName(PORT)
            .hasArg()
            .withDescription("Port of MarkLogic Server")
            .create(PORT);
        options.addOption(port);
        Option db = OptionBuilder
            .withArgName(DATABASE)
            .hasArg()
            .withDescription("Database of MarkLogic Server")
            .create(DATABASE);
        options.addOption(db);
    }

    static void configCopyOptions(Options options) {
        Option cpcol = OptionBuilder
            .withArgName("true,false")
            .hasOptionalArg()
            .withDescription("Whether to copy document collections from source"
                   + " to destination")
            .create(COPY_COLLECTIONS);
        options.addOption(cpcol);
        Option cppm = OptionBuilder
            .withArgName("true,false")
            .hasOptionalArg()
            .withDescription("Whether to copy document permissions from source"
                   + " to destination")
            .create(COPY_PERMISSIONS);
        options.addOption(cppm);
        Option cppt = OptionBuilder
            .withArgName("true,false")
            .hasOptionalArg()
            .withDescription("Whether to copy document properties from source"
                   + " to destination")
            .create(COPY_PROPERTIES);
        options.addOption(cppt);
        Option cpqt = OptionBuilder
            .withArgName("true,false")
            .hasOptionalArg()
            .withDescription("Whether to copy document quality from source"
                   + " to destination")
            .create(COPY_QUALITY);
        options.addOption(cpqt);
    }

    static void configBatchTxn(Options options) {
        Option batchSize = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription("Number of document in one request")
            .create(BATCH_SIZE);
        options.addOption(batchSize);
        Option txnSize = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription("Number of requests in one transaction")
            .create(TRANSACTION_SIZE);
        options.addOption(txnSize);
    }
    
    static void configModule(Options options) {
        Option moduleUri = OptionBuilder
            .withArgName("String")
            .hasArg()
            .withDescription(
                "Path to the module containing the transform function")
            .create(TRANSFORM_MODULE);
        options.addOption(moduleUri);
        Option ns = OptionBuilder.withArgName("String").hasArg()
            .withDescription("Namespace of the transform function")
            .create(TRANSFORM_NAMESPACE);
        options.addOption(ns);
        Option func = OptionBuilder.withArgName("String").hasArg()
            .withDescription("Name of the transform function")
            .create(TRANSFORM_FUNCTION);
        options.addOption(func);
        Option param = OptionBuilder.withArgName("String").hasArg()
            .withDescription("Name of the transform function")
            .create(TRANSFORM_PARAM);
        options.addOption(param);
    }
    
    static void configPartition(Options options) {
        Option partition = OptionBuilder.withArgName("partition name")
            .hasArg().withDescription("The partition where docs are inserted")
            .create(OUTPUT_PARTITION);
        options.addOption(partition);
    }

    static void configFilteringOptions(Options options) {
        Option df = OptionBuilder
            .withArgName("String")
            .hasArg()
            .withDescription("Comma-separated list of directories")
            .create(DIRECTORY_FILTER);
        options.addOption(df);
        Option cf = OptionBuilder
            .withArgName("String")
            .hasArg()
            .withDescription("Comma-separated list of collections")
            .create(COLLECTION_FILTER);
        options.addOption(cf);
        Option ds = OptionBuilder
            .withArgName("String")
            .hasArg()
            .withDescription("Path expression used to retrieve documents or " +
                    "element nodes from the server")
            .create(DOCUMENT_SELECTOR);
        options.addOption(ds);
    }
    
    static void applyModuleConfigOptions(Configuration conf,
        CommandLine cmdline, String batchSize) {
        if (cmdline.hasOption(TRANSFORM_MODULE)) {
            if (batchSize !=null && Integer.valueOf(batchSize)> 1) {
                //batch size is set explicitly to > 1
                throw new UnsupportedOperationException(
                    "Server-side transformation can't work with batch size "
                        + "greater than 1");
            }
            if(batchSize == null) {
                conf.setInt(MarkLogicConstants.BATCH_SIZE, 1);
            }
            if (conf.getBoolean(MarkLogicConstants.OUTPUT_STREAMING, false) == true) {
                throw new UnsupportedOperationException(
                    "Server-side transformation can't work with streaming");
            }
            String arg = cmdline.getOptionValue(TRANSFORM_MODULE);
            conf.set(CONF_TRANSFORM_MODULE, arg);
            
            if (cmdline.hasOption(TRANSFORM_NAMESPACE)) {
                arg = cmdline.getOptionValue(TRANSFORM_NAMESPACE);
                conf.set(CONF_TRANSFORM_NAMESPACE, arg);
            }
            if (cmdline.hasOption(TRANSFORM_FUNCTION)) {
                arg = cmdline.getOptionValue(TRANSFORM_FUNCTION);
                conf.set(CONF_TRANSFORM_FUNCTION, arg);
            }
            if (cmdline.hasOption(TRANSFORM_PARAM)) {
                arg = cmdline.getOptionValue(TRANSFORM_PARAM);
                conf.set(CONF_TRANSFORM_PARAM, arg);
            }
        }

    }
    
    static void applyPartitionConfigOptions(Configuration conf,
        CommandLine cmdline) {
        if (cmdline.hasOption(OUTPUT_PARTITION)) {
            String arg = cmdline.getOptionValue(OUTPUT_PARTITION);
            conf.set(MarkLogicConstants.OUTPUT_PARTITION, arg);
        }
    }

    static void applyCopyConfigOptions(Configuration conf, CommandLine cmdline) {
        if (cmdline.hasOption(COPY_COLLECTIONS)) {
            String arg = cmdline.getOptionValue(COPY_COLLECTIONS);
            if (arg == null || arg.equalsIgnoreCase("true")) {
                conf.setBoolean(CONF_COPY_COLLECTIONS, true);
            } else if (arg.equalsIgnoreCase("false")) {
                conf.setBoolean(CONF_COPY_COLLECTIONS, false);
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized option argument for " + COPY_COLLECTIONS
                                + ": " + arg);
            }
        } else {
            conf.set(CONF_COPY_COLLECTIONS, DEFAULT_COPY_COLLECTIONS);
        }
        if (cmdline.hasOption(COPY_PERMISSIONS)) {
            String arg = cmdline.getOptionValue(COPY_PERMISSIONS);
            if (arg == null || arg.equalsIgnoreCase("true")) {
                conf.setBoolean(CONF_COPY_PERMISSIONS, true);
            } else if (arg.equalsIgnoreCase("false")) {
                conf.setBoolean(CONF_COPY_PERMISSIONS, false);
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized option argument for " + COPY_PERMISSIONS
                                + ": " + arg);
            }
        } else {
            conf.set(CONF_COPY_PERMISSIONS, DEFAULT_COPY_PERMISSIONS);
        }
        if (cmdline.hasOption(COPY_PROPERTIES)) {
            String arg = cmdline.getOptionValue(COPY_PROPERTIES);
            if (arg == null || arg.equalsIgnoreCase("true")) {
                conf.setBoolean(CONF_COPY_PROPERTIES, true);
            } else {
                conf.setBoolean(CONF_COPY_PROPERTIES, false);
            }
        } else {
            conf.set(CONF_COPY_PROPERTIES, DEFAULT_COPY_PROPERTIES);
        }
        if (cmdline.hasOption(COPY_QUALITY)) {
            String arg = cmdline.getOptionValue(COPY_QUALITY);
            if (arg == null || arg.equalsIgnoreCase("true")) {
                conf.setBoolean(CONF_COPY_QUALITY, true);
            } else if (arg.equalsIgnoreCase("false")) {
                conf.setBoolean(CONF_COPY_QUALITY, false);
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized option argument for " + COPY_QUALITY
                                + ": " + arg);
            }
        } else {
            conf.set(CONF_COPY_QUALITY, DEFAULT_COPY_QUALITY);
        }
    }

    static void applyFilteringConfigOptions(Configuration conf,
                    CommandLine cmdline) {
        int filters = cmdline.hasOption(COLLECTION_FILTER) ? 1 : 0;
        filters += cmdline.hasOption(DIRECTORY_FILTER) ? 1 : 0;
        filters += cmdline.hasOption(DOCUMENT_SELECTOR) ? 1 : 0;
        if (filters > 1) {
            LOG.error("Only one of " + COLLECTION_FILTER + ", " +
                    DIRECTORY_FILTER + ", " + DOCUMENT_SELECTOR +
                    " can be specified.");
            throw new IllegalArgumentException(
                    "Only one of " + COLLECTION_FILTER + ", " +
                    DIRECTORY_FILTER + ", " + DOCUMENT_SELECTOR +
                    " can be specified.");
        }
        
        if (cmdline.hasOption(COLLECTION_FILTER)) {
            String c = cmdline.getOptionValue(COLLECTION_FILTER);
            String[] cf = c.split(",");
            if (cf.length > 1) {
                StringBuilder sb = new StringBuilder("(");
                for (int i = 0; i < cf.length; i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append("\"");
                    sb.append(cf[i]);
                    sb.append("\"");
                }
                sb.append(")");
                conf.set(ConfigConstants.CONF_COLLECTION_FILTER, sb.toString());
                conf.set(MarkLogicConstants.DOCUMENT_SELECTOR,
                    "fn:collection(" + sb.toString() + ")");
            } else {
                conf.set(ConfigConstants.CONF_COLLECTION_FILTER, "\"" + c + "\"");
                conf.set(MarkLogicConstants.DOCUMENT_SELECTOR,
                    "fn:collection(\"" + c + "\")");
            }
        }
        if (cmdline.hasOption(DIRECTORY_FILTER)) {
            String d = cmdline.getOptionValue(DIRECTORY_FILTER);
            String[] df = d.split(",");
            if (df.length > 1) {
                StringBuilder sb = new StringBuilder("(");
                for (int i = 0; i < df.length; i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    if (!df[i].endsWith("/")) {
                        LOG.warn("directory_filter: Directory does not end "
                            + "with a forward slash (/): " + df[i]);
                    }
                    sb.append("\"");
                    sb.append(df[i]);
                    sb.append("\"");
                }
                sb.append(")");
                conf.set(ConfigConstants.CONF_DIRECTORY_FILTER, sb.toString());
                conf.set(MarkLogicConstants.DOCUMENT_SELECTOR,
                    "xdmp:directory(" + sb.toString() + ",\"infinity\")");
            } else {
                if (!d.endsWith("/")) {
                    LOG.warn("directory_filter: Directory does not end "
                        + "with a forward slash (/): " + d);
                }
                conf.set(ConfigConstants.CONF_DIRECTORY_FILTER, "\"" + d
                    + "\"");
                conf.set(MarkLogicConstants.DOCUMENT_SELECTOR,
                    "xdmp:directory(\"" + d + "\",\"infinity\")");
            }
        }
        if (cmdline.hasOption(DOCUMENT_SELECTOR)) {
            conf.set(MarkLogicConstants.DOCUMENT_SELECTOR, 
                    cmdline.getOptionValue(DOCUMENT_SELECTOR));
        }
    }

    static void applyCommonOutputConfigOptions(Configuration conf,
                    CommandLine cmdline) {

        if (cmdline.hasOption(OUTPUT_URI_REPLACE)) {
            String uriReplace = cmdline.getOptionValue(OUTPUT_URI_REPLACE);
            if (uriReplace == null) {
                throw new IllegalArgumentException("Missing option argument: "
                        + OUTPUT_URI_REPLACE);
            } else {
                String[] replace = uriReplace.split(",");
                // URI replace comes in pattern and replacement pairs.
                if (replace.length % 2 != 0) {
                    throw new IllegalArgumentException(
                            "Invalid option argument for "
                            + OUTPUT_URI_REPLACE + " :" + uriReplace);
                }
                // Replacement string is expected to be in ''
                for (int i = 0; i < replace.length - 1; i++) {
                    String replacement = replace[++i].trim();
                    if (!replacement.startsWith("'") || 
                        !replacement.endsWith("'")) {
                        throw new IllegalArgumentException(
                                "Invalid option argument for "
                                + OUTPUT_URI_REPLACE + " :" + uriReplace);
                    }
                }
                conf.setStrings(CONF_OUTPUT_URI_REPLACE, uriReplace);
            }
        }
        if (cmdline.hasOption(OUTPUT_URI_PREFIX)) {
            String outPrefix = cmdline.getOptionValue(OUTPUT_URI_PREFIX);
            conf.set(CONF_OUTPUT_URI_PREFIX, outPrefix);
        }
        if (cmdline.hasOption(OUTPUT_URI_SUFFIX)) {
            String outSuffix = cmdline.getOptionValue(OUTPUT_URI_SUFFIX);
            conf.set(CONF_OUTPUT_URI_SUFFIX, outSuffix);
        }

        if (cmdline.hasOption(OUTPUT_COLLECTIONS)) {
            String collectionsString = cmdline.getOptionValue(
                    OUTPUT_COLLECTIONS);
            conf.set(MarkLogicConstants.OUTPUT_COLLECTION, collectionsString);
        }
        if (cmdline.hasOption(OUTPUT_PERMISSIONS)) {
            String permissionString = cmdline.getOptionValue(
                    OUTPUT_PERMISSIONS);
            conf.set(MarkLogicConstants.OUTPUT_PERMISSION, permissionString);
        }
        if (cmdline.hasOption(OUTPUT_QUALITY)) {
            String quantity = cmdline.getOptionValue(OUTPUT_QUALITY);
            conf.set(MarkLogicConstants.OUTPUT_QUALITY, quantity);
        }

        if (cmdline.hasOption(RDF_STREAMING_MEMORY_THRESHOLD)) {
            String thresh = cmdline.getOptionValue(RDF_STREAMING_MEMORY_THRESHOLD);
            conf.set(RDF_STREAMING_MEMORY_THRESHOLD, thresh);
        }
        if (cmdline.hasOption(RDF_TRIPLES_PER_DOCUMENT)) {
            String count = cmdline.getOptionValue(RDF_TRIPLES_PER_DOCUMENT);
            conf.set(RDF_TRIPLES_PER_DOCUMENT, count);
        }
    }
    
    static InputType getInputType(CommandLine cmdline) {
        String inputTypeOption = cmdline.getOptionValue(INPUT_FILE_TYPE,
                INPUT_FILE_TYPE_DEFAULT);
        return InputType.forName(inputTypeOption);
    }

    public void printUsage(Command cmd, Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(HelpFormatter.DEFAULT_WIDTH, 
                cmd.name(), null, options, null, true);
    }
}
