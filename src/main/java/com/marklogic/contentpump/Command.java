/*
 * Copyright (c) 2024 MarkLogic Corporation
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
import java.util.Random;

import com.marklogic.contentpump.utilities.CommandlineOption;
import com.marklogic.http.HttpChannel;
import com.marklogic.contentpump.utilities.AuditUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
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
            configRDFGraphOutputOptions(options);
            configMLCloudAuthOptions(options);
            
			Option inputFilePath = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("The file system location for input, as a "
                        + "regular expression")
                .create(INPUT_FILE_PATH);
            inputFilePath.setRequired(true);
            options.addOption(inputFilePath);
            Option inputFilePattern = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Matching regex pattern for files found in "
                    + "the input file path")
                .create(INPUT_FILE_PATTERN);
            options.addOption(inputFilePattern);
            Option aggregateRecordElement = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Element name in which each document is "
                        + "found")
                .create(AGGREGATE_RECORD_ELEMENT);
            options.addOption(aggregateRecordElement);
            Option aggregateRecordNamespace = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Element namespace in which each document "
                        + "is found")
                .create(AGGREGATE_RECORD_NAMESPACE);
            options.addOption(aggregateRecordNamespace);
            Option aggregateUriId = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Deprecated. Name of the first element or attribute "
                        + "within a record element to be used as document URI."
                        + " If omitted, a sequence id will be generated to "
                        + " form the document URI.")                      
                .create(AGGREGATE_URI_ID);
            options.addOption(aggregateUriId);
            Option inputFileType = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Type of input file.  Valid choices are: "
                    + "aggregates, archive, delimited_text, documents, forest,"
                    + "rdf, sequencefile, delimited_json")
                .create(INPUT_FILE_TYPE);
            options.addOption(inputFileType);
            Option inputCompressed = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether the input data is compressed")
                .create(INPUT_COMPRESSED);
            options.addOption(inputCompressed);
            Option inputCompressionCodec = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Codec used for compression: ZIP, GZIP")
                .create(INPUT_COMPRESSION_CODEC);
            options.addOption(inputCompressionCodec);
            Option documentType = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Type of document content. Valid choices: "
                    + "XML, JSON, TEXT, BINARY, and MIXED.  Default type for " 
                    + "document is MIXED, where the type is determined "
                    + "from the MIME type mapping configured in MarkLogic "
                    + "Server.")
                .create(DOCUMENT_TYPE);
            options.addOption(documentType);
            Option delimiter = OptionBuilder
                .withArgName("character")
                .hasArg()
                .withDescription("Delimiter for delimited text.")
                .create(DELIMITER);
            options.addOption(delimiter);
            Option delimitedUri = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Deprecated. Delimited uri id for delimited text.")
                .create(DELIMITED_URI_ID);
            options.addOption(delimitedUri);
            Option delimitedRoot = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Root element local name of the XML " +
                        "document constructed from one delimited text record.")
                .create(DELIMITED_ROOT_NAME);
            options.addOption(delimitedRoot);
            Option generateUri = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Enables automatic URI generation for " +
                        "delimited text records.")
                .create(GENERATE_URI);
            options.addOption(generateUri);
            Option namespace = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Namespace used for output document.")
                .create(NAMESPACE);
            options.addOption(namespace);
            Option outputLanguage = OptionBuilder
                .withArgName("string")
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
                .withArgName("string")
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
                .withArgName("string")
                .hasArg()
                .withDescription("Whether to repair documents to make it well "
                        + "formed or throw error.")
                .create(XML_REPAIR_LEVEL);
            options.addOption(repairLevel);
            Option seqKeyClass = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Name of class to be used as key to read the "
                        + " input SequenceFile")
                .create(INPUT_SEQUENCEFILE_KEY_CLASS);
            options.addOption(seqKeyClass);
            Option seqValueClass = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Name of class to be used as value to read "
                        + "the input SequenceFile")
                .create(INPUT_SEQUENCEFILE_VALUE_CLASS);
            options.addOption(seqValueClass);
            Option seqValueType = OptionBuilder
                .withArgName("string")
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
                .withArgName("string")
                .hasArg()
                .withDescription(
                    "The charset encoding to be used by the MarkLogic when "
                        + "loading documents.  The default is \"UTF-8\".")
                .create(CONTENT_ENCODING);
            options.addOption(encoding);
            Option uriId = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription("A column name in delimited text file "
                            + "or an element name in aggregated XML "
                            + "or a property name in delimited json, "
                            + "whose value will be the document uri in MarkLogic Server.")
                    .create(URI_ID);
            options.addOption(uriId);
            Option dataType = OptionBuilder
                    .withArgName("comma list")
                    .hasArg()
                    .withDescription("Comma separated list of column name "
                            + " and data type pairs. 1st to match column name,"
                            + " case sensitive. 2nd the data type, case insensitive."
                            + "Data type can be String, Number or Boolean.")
                    .create(DATA_TYPE);
            options.addOption(dataType);
            Option threadsPerSplit = OptionBuilder
                    .withArgName("number")
                    .hasArg()
                    .withDescription("The number of threads per split")
                    .create(THREADS_PER_SPLIT);
            options.addOption(threadsPerSplit);

            Option rdfMemoryThreshold_opt = OptionBuilder
                    .withArgName("number")
                    .hasArg()
                    .withDescription("Maximum size of an RDF document to be processed in memory")
                    .create(RDF_STREAMING_MEMORY_THRESHOLD);
            CommandlineOption rdfMemoryThreshold = new CommandlineOption(rdfMemoryThreshold_opt);
            rdfMemoryThreshold.setHidden(true);
            options.addOption(rdfMemoryThreshold);

            Option rdfTriplesPerDoc_opt = OptionBuilder
                    .withArgName("number")
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
                .withArgName("comma list")
                .hasArg()
                .withDescription("Comma-separated list of directories")
                .create(DIRECTORY_FILTER);
            options.addOption(df);
            Option cf = OptionBuilder
                .withArgName("comma list")
                .hasArg()
                .withDescription("Comma-separated list of collections")
                .create(COLLECTION_FILTER);
            options.addOption(cf);
            Option tf = OptionBuilder
                .withArgName("comma list")
                .hasArg()
                .withDescription("Comma-separated list of document types")
                .create(TYPE_FILTER);
            options.addOption(tf);
            Option tcf = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("temporal collection name")
                .create(TEMPORAL_COLLECTION);
            options.addOption(tcf);
            Option maxThreads = OptionBuilder
                .withArgName("number")
                .hasArg()
                .withDescription("The maximum number of threads allowed " +
                    "running on the client side")
                .create(MAX_THREADS);
            options.addOption(maxThreads);
            Option maxThreadPercentage = OptionBuilder
                .withArgName("number")
                .hasArg()
                .withDescription("The maximum percentage (between 0 and 100) " +
                    "of available server threads used by the client for " +
                    "running mlcp requests.")
                .create(MAX_THREAD_PERCENTAGE);
            options.addOption(maxThreadPercentage);
            Option pollingPeriod = OptionBuilder
                .withArgName("number")
                .hasArg()
                .withDescription("The period (in minutes) mlcp talks to the " +
                    "server to collect maximum available server threads and " +
                    "decides whether to auto-scale.")
                .create(POLLING_PERIOD);
            options.addOption(pollingPeriod);
            Option pollingInitDelay = OptionBuilder
                .withArgName("number")
                .hasArg()
                .withDescription("The initial delay (in minutes) before mlcp " +
                    "starts running polling requests to the server to collect" +
                    "maximum available server threads for auto-scaling.")
                .create(POLLING_INIT_DELAY);
            options.addOption(pollingInitDelay);
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
                        throws IOException {
            applyConfigOptions(conf, cmdline);
            InputType type = getInputType(cmdline);
            type.applyConfigOptions(conf, cmdline);
            
            // construct a job
            Job job = LocalJob.getInstance(conf);
            
            Class<?> inputFormatClass = 
                    conf.getClass(JobContext.INPUT_FORMAT_CLASS_ATTR, null);
            if (inputFormatClass == null) {
                job.setInputFormatClass(type.getInputFormatClass(cmdline, conf));
            }
            Class<?> outputFormatClass = 
                    conf.getClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR, null);
            if (outputFormatClass == null) {
                job.setOutputFormatClass(type.getOutputFormatClass(cmdline, conf));
            }
            
            job.setJobName(getNewJobName(conf));

            // set mapper class
            Class<?> mapperClass = 
                    conf.getClass(JobContext.MAP_CLASS_ATTR, null);
            if (mapperClass == null) { 
                setMapperClass(job, conf, cmdline);
            }
            
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

        void applyUriId(Configuration conf, InputType inputType, CommandLine cmdline) {
            String uriId = null;
            if (cmdline.hasOption(DELIMITED_URI_ID)) {
                LOG.warn(DELIMITED_URI_ID + " has been depracated, use " + URI_ID);
                uriId = cmdline.getOptionValue(DELIMITED_URI_ID);
            }
            if (cmdline.hasOption(AGGREGATE_URI_ID)) {
                LOG.warn(AGGREGATE_URI_ID + " has been depracated, use " + URI_ID);
                uriId = cmdline.getOptionValue(AGGREGATE_URI_ID);
            }
            if (cmdline.hasOption(URI_ID)) {
                uriId = cmdline.getOptionValue(URI_ID);
            }
            String generate = null;
            if (cmdline.hasOption(GENERATE_URI)) {
                generate = cmdline.getOptionValue(GENERATE_URI);
                if (generate == null) {
                    generate = "true";
                }
                if (!"true".equalsIgnoreCase(generate) && 
                        !"false".equalsIgnoreCase(generate)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                                    GENERATE_URI + ": " + generate);
                }
            }
            
            if (uriId != null) {
                if (InputType.AGGREGATES == inputType ||
                        InputType.DELIMITED_JSON == inputType ||
                        InputType.DELIMITED_TEXT == inputType) {
                    conf.set(CONF_INPUT_URI_ID, uriId);
                    if (InputType.AGGREGATES != inputType && 
                            generate != null &&
                            "true".equalsIgnoreCase(generate)) {
                        throw new IllegalArgumentException("Only one of " + GENERATE_URI
                                + " and " + URI_ID + " can be specified");
                    }
                } else {
                    throw new IllegalArgumentException(
                            URI_ID + 
                            " is not applicable to " + inputType.name());
                }
            } else {
                if (InputType.DELIMITED_TEXT == inputType) {
                    if ("true".equalsIgnoreCase(generate)) {
                        conf.setBoolean(CONF_INPUT_GENERATE_URI, true);
                    }
                } else if (InputType.DELIMITED_JSON == inputType) {
                    if (generate != null && "false".equalsIgnoreCase(generate)) {
                        throw new IllegalArgumentException(GENERATE_URI + " must be true if "
                                + URI_ID + " not specified");
                    } else {
                        conf.setBoolean(CONF_INPUT_GENERATE_URI, true);
                    }
                }
            }
        }
        
        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyCommonOutputConfigOptions(conf, cmdline);
            applyRDFGraphOutputConfigOptions(conf, cmdline);
            
            InputType inputType = getInputType(cmdline);   
            ContentType contentType = inputType.getContentType(cmdline);
            
            if (InputType.DELIMITED_TEXT == inputType 
                    && ContentType.XML != contentType 
                    && contentType.JSON != contentType) {
                throw new IllegalArgumentException("The setting for " + DOCUMENT_TYPE + "is not applicable to " + inputType);
            }
            
            applyUriId(conf, inputType, cmdline);
            
            if (cmdline.hasOption(DOCUMENT_TYPE)
                    && InputType.DOCUMENTS != inputType
                    && InputType.DELIMITED_TEXT != inputType) {
                LOG.warn(DOCUMENT_TYPE + " is not supported for " + inputType.name());
            }
            if (cmdline.hasOption(DATA_TYPE)) {
                if (InputType.DELIMITED_TEXT != inputType) {
                    throw new IllegalArgumentException(DATA_TYPE + " is only applicable to "
                            + InputType.DELIMITED_TEXT.name());
                }
                String type = cmdline.getOptionValue(DOCUMENT_TYPE, 
                        ContentType.XML.name());
                if ("XML".equalsIgnoreCase(type)) {
                    LOG.warn(DATA_TYPE + " is only applicable when "
                            + DOCUMENT_TYPE + " is " 
                            + ContentType.JSON.name());
                } else {
                    String value = cmdline.getOptionValue(DATA_TYPE);
                    String[] types = value.split(",");
                
                    if (types.length%2 != 0) {
                        throw new IllegalArgumentException("Invalid option argument for "
                                + DATA_TYPE + ": " + value);
                    }
                    conf.set(CONF_DELIMITED_DATA_TYPE, value);
                }
            }
            
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
                if (isNullOrEqualsTrue(arg)) {
                    conf.setBoolean(CONF_INPUT_ARCHIVE_METADATA_OPTIONAL,
                                    true);
                } else if ("false".equalsIgnoreCase(arg)) {
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
                if (delim == null || delim.length() != 1) {
                    throw new IllegalArgumentException("Invalid delimiter: " +
                            delim); 
                }
                conf.set(CONF_DELIMITER, delim);
            }
            if (cmdline.hasOption(DELIMITED_ROOT_NAME)) {
                String type = cmdline.getOptionValue(DOCUMENT_TYPE, 
                        ContentType.XML.name());
                if ("JSON".equalsIgnoreCase(type)) {
                    LOG.warn(DELIMITED_ROOT_NAME 
                            + " is only applicable when " 
                            + DOCUMENT_TYPE + " is " 
                            + ContentType.XML.name());
                } else {
                    String delimRoot = cmdline
                            .getOptionValue(DELIMITED_ROOT_NAME);
                    conf.set(CONF_DELIMITED_ROOT_NAME, delimRoot);
                }
            }
            if (cmdline.hasOption(OUTPUT_FILENAME_AS_COLLECTION)) {
                String arg = cmdline.getOptionValue(
                        OUTPUT_FILENAME_AS_COLLECTION);
                conf.setBoolean(CONF_OUTPUT_FILENAME_AS_COLLECTION, isNullOrEqualsTrue(arg));
            }
            if (cmdline.hasOption(OUTPUT_DIRECTORY)) {
                String outDir = cmdline.getOptionValue(OUTPUT_DIRECTORY);
                conf.set(MarkLogicConstants.OUTPUT_DIRECTORY, outDir);
            }
            if (cmdline.hasOption(OUTPUT_CLEANDIR)) {
                String arg = cmdline.getOptionValue(OUTPUT_CLEANDIR);
                if (isNullOrEqualsTrue(arg)) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_CLEAN_DIR, true);
                } else if ("false".equalsIgnoreCase(arg)) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_CLEAN_DIR, 
                            false);
                } else {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                            OUTPUT_CLEANDIR + ": " + arg);
                }
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
            if (cmdline.hasOption(BASE_PATH)) {
                String basePath = cmdline.getOptionValue(BASE_PATH);
                conf.set(MarkLogicConstants.OUTPUT_BASE_PATH, basePath);
            }
            if (cmdline.hasOption(API_KEY)) {
                String apiKey = cmdline.getOptionValue(API_KEY);
                conf.set(MarkLogicConstants.OUTPUT_API_KEY, apiKey);
                // When api key is specified, we are expecting a non-empty
                // base path.
                if (conf.get(MarkLogicConstants.OUTPUT_BASE_PATH) == null) {
                    throw new IllegalArgumentException("Option " + BASE_PATH +
                    " cannot be empty when api key is specified.");
                }
            }
            String port = DEFAULT_PORT;
            if (cmdline.hasOption(PORT)) {
                port = cmdline.getOptionValue(PORT);
            }
            // If connecting to ML Cloud, ignore port input and use default 443
            if (conf.get(MarkLogicConstants.OUTPUT_API_KEY) != null) {
                port = DEFAULT_ML_CLOUD_PORT;
            }
            conf.set(MarkLogicConstants.OUTPUT_PORT, port);
            if (cmdline.hasOption(HOST)) {
                String hosts = cmdline.getOptionValue(HOST);
                InternalUtilities.verifyHosts(
                    hosts, conf.get(MarkLogicConstants.OUTPUT_PORT));
                conf.set(MarkLogicConstants.OUTPUT_HOST, hosts);
            }
            if (cmdline.hasOption(RESTRICT_HOSTS)) {
                String restrict = cmdline.getOptionValue(RESTRICT_HOSTS);
                if (isNullOrEqualsTrue(restrict)) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_RESTRICT_HOSTS, true);
                    HttpChannel.setUseHTTP(true);
                } else if (!"false".equalsIgnoreCase(restrict)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                                    RESTRICT_OUTPUT_HOSTS + ": " + restrict);
                }
            } else { // use HTTP compliant mode to true for initial probing
                HttpChannel.setUseHTTP(true);
            }
            // If base path is specified, it means that ML server is sitting
            // behind a reverse proxy. Ignore input from -restrict_hosts and
            // enable restrict_hosts and http compliant mode
            if (conf.get(MarkLogicConstants.OUTPUT_BASE_PATH) != null) {
                conf.setBoolean(MarkLogicConstants.OUTPUT_RESTRICT_HOSTS, true);
                HttpChannel.setUseHTTP(true);
            }
            if (cmdline.hasOption(DATABASE)) {
                String db = cmdline.getOptionValue(DATABASE);
                conf.set(MarkLogicConstants.OUTPUT_DATABASE_NAME, db);
            }
            if (cmdline.hasOption(SSL)) {
                String arg = cmdline.getOptionValue(SSL);
                if (isNullOrEqualsTrue(arg)){
                    conf.setBoolean(MarkLogicConstants.OUTPUT_USE_SSL, true);
                } else if (!"false".equalsIgnoreCase(arg)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + SSL
                            + ": " + arg);
                }
            }
            // If api key is specified, ignore input from -ssl and enable ssl
            if (conf.get(MarkLogicConstants.OUTPUT_API_KEY) != null) {
                conf.setBoolean(MarkLogicConstants.OUTPUT_USE_SSL, true);
            }
            applyProtocol(conf, cmdline, SSL_PROTOCOL, MarkLogicConstants.OUTPUT_SSL_PROTOCOL);
            if (cmdline.hasOption(KEYSTORE_PATH)) {
                String path = cmdline.getOptionValue(KEYSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.OUTPUT_KEYSTORE_PATH, path);
            }
            if (cmdline.hasOption(KEYSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(KEYSTORE_PASSWD);
                conf.set(MarkLogicConstants.OUTPUT_KEYSTORE_PASSWD, passwd);
            }
            if (cmdline.hasOption(TRUSTSTORE_PATH)) {
                String path = cmdline.getOptionValue(TRUSTSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.OUTPUT_TRUSTSTORE_PATH, path);
            }
            if (cmdline.hasOption(TRUSTSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(TRUSTSTORE_PASSWD);
                conf.set(MarkLogicConstants.OUTPUT_TRUSTSTORE_PASSWD, passwd);
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
                if (SequenceFileValueType.BYTESWRITABLE.toString()
                    .equalsIgnoreCase(valueType)) {
                        conf.set(MarkLogicConstants.CONTENT_TYPE,
                                    ContentType.BINARY.toString());
                }
            } else if (conf.get(CONF_INPUT_SEQUENCEFILE_VALUE_TYPE) == null) {
                conf.set(CONF_INPUT_SEQUENCEFILE_VALUE_TYPE,
                                DEFAULT_SEQUENCEFILE_VALUE_TYPE);
            }
            if (cmdline.hasOption(INPUT_FILE_TYPE)) {
                String fileType = cmdline.getOptionValue(INPUT_FILE_TYPE);
                if (InputType.ARCHIVE.toString().equalsIgnoreCase(fileType)) {
                    conf.set(MarkLogicConstants.CONTENT_TYPE,
                                    ContentType.UNKNOWN.toString());
                }
            }
            if (cmdline.hasOption(FAST_LOAD)) {
                String arg = cmdline.getOptionValue(FAST_LOAD);
                if (isNullOrEqualsTrue(arg)) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, true);
                    LOG.info("Option fastload is specified.Please make sure "+
                             "that all conditions required to run in fastload "+
                             "mode are satisfied to avoid XDMP-DBDUPURI "+
                             "errors.");
                } else if ("false".equalsIgnoreCase(arg)) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, false);
                } else {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + FAST_LOAD
                                    + ": " + arg);
                }
            }
            if (cmdline.hasOption(CONTENT_ENCODING)) {
                String arg = cmdline.getOptionValue(CONTENT_ENCODING).toUpperCase();
                if ("SYSTEM".equals(arg)) {
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
                            ") for the job.");
                }
            }
            if (cmdline.hasOption(MAX_THREADS)) {
                String arg = cmdline.getOptionValue(MAX_THREADS);
                int maxThreads = Integer.parseInt(arg);
                if (maxThreads < inputType.getMinThreads()) {
                    throw new IllegalArgumentException("Cannot set " +
                        MAX_THREADS + " to a value less than the minimum " +
                        "required threads (" + inputType.getMinThreads() +
                        ") for the job.");
                }
            }
            if (cmdline.hasOption(MAX_THREAD_PERCENTAGE)) {
                String arg = cmdline.getOptionValue(MAX_THREAD_PERCENTAGE);
                int maxThreadPercentage = Integer.parseInt(arg);
                if (maxThreadPercentage <= 0 || maxThreadPercentage > 100) {
                    throw new IllegalArgumentException("Illegal percentage " +
                        "set for " + MAX_THREAD_PERCENTAGE + ". Please " +
                        "specify a number between 0 and 100");
                }
            }
            if (cmdline.hasOption(TEMPORAL_COLLECTION)) {
                String fileType = cmdline.getOptionValue(INPUT_FILE_TYPE);
                if (fileType != null &&
                    InputType.RDF.toString().equalsIgnoreCase(fileType)) {
                    throw new IllegalArgumentException(
                        "Cannot ingest RDF into temporal collection");
                }
            }
            applyPartitionConfigOptions(conf, cmdline);
        
            applyModuleConfigOptions(conf, cmdline);
            applyBatchTxn(conf, cmdline, MAX_BATCH_SIZE);
            
            if (cmdline.hasOption(SPLIT_INPUT)) {
                String arg = cmdline.getOptionValue(SPLIT_INPUT);
                if (isNullOrEqualsTrue(arg)) {
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
                } else if ("false".equalsIgnoreCase(arg)) {
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
                    conf.set(MarkLogicConstants.COLLECTION_FILTER, colFilter);
                } else {
                    LOG.warn("The setting for " + COLLECTION_FILTER + 
                            " is not applicable for " + inputType); 
                }
            }
            if (cmdline.hasOption(DIRECTORY_FILTER)) {
                if (inputType == InputType.FOREST) {
                    String dirFilter = 
                            cmdline.getOptionValue(DIRECTORY_FILTER);
                    conf.set(MarkLogicConstants.DIRECTORY_FILTER, dirFilter);
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
            // Hidden parameters for MarkLogic Cloud
            if (cmdline.hasOption(TOKEN_ENDPOINT)) {
                String tokenEndpoint = cmdline.getOptionValue(TOKEN_ENDPOINT);
                conf.set(MarkLogicConstants.OUTPUT_TOKEN_ENDPOINT, tokenEndpoint);
            }
            if (cmdline.hasOption(GRANT_TYPE)) {
                String grantType = cmdline.getOptionValue(GRANT_TYPE);
                conf.set(MarkLogicConstants.OUTPUT_GRANT_TYPE, grantType);
            }
            if (cmdline.hasOption(TOKEN_DURATION)) {
                String tokenDuration = cmdline.getOptionValue(TOKEN_DURATION);
                conf.set(MarkLogicConstants.OUTPUT_TOKEN_DURATION, tokenDuration);
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
        public Class<? extends Mapper<?,?,?,?>> getRuntimeMapperClass(
            Job job, Class<? extends Mapper<?,?,?,?>> mapper,
            int threadsPerSplit) {
			if (threadsPerSplit == 0 && !job.getConfiguration().getBoolean(
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
            configRedactionOptions(options);
            configMLCloudAuthOptions(options);

            Option outputType = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("export output type")
                .create(OUTPUT_TYPE);
            options.addOption(outputType);
            Option outputFilePath = OptionBuilder
                .withArgName("string")
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
                .withDescription("Whether to format XML data with indentation")
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
                .withArgName("string")
                .hasArg()
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
                if (isNullOrEqualsTrue(arg)){
                    setQueryTimestamp(conf);
                } else if (!"false".equalsIgnoreCase(arg)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + SNAPSHOT
                            + ": " + arg);
                }
            }
            
            // construct a job
            Job job = LocalJob.getInstance(conf);
            job.setJarByClass(this.getClass());
            Class<?> inputFormatClass = 
                    conf.getClass(JobContext.INPUT_FORMAT_CLASS_ATTR, null);
            if (inputFormatClass == null) {
                job.setInputFormatClass(outputType.getInputFormatClass());
            }
            Class<?> mapperClass = 
                    conf.getClass(JobContext.MAP_CLASS_ATTR, null);
            if (mapperClass == null) {
                setMapperClass(job, conf, cmdline);
            }
            Class<?> mapOutputKeyClass = 
                    conf.getClass(JobContext.MAP_OUTPUT_KEY_CLASS, null);
            if (mapOutputKeyClass == null) {
                job.setMapOutputKeyClass(DocumentURI.class);
            }
            Class<?> mapOutputValueClass = 
                    conf.getClass(JobContext.MAP_OUTPUT_VALUE_CLASS, null);
            if (mapOutputValueClass == null) {
                job.setMapOutputValueClass(MarkLogicDocument.class);
            }
            Class<?> outputFormatClass = 
                    conf.getClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR, null);
            if (outputFormatClass == null) {
                job.setOutputFormatClass(
                            outputType.getOutputFormatClass(cmdline));
            }
            Class<?> outputKeyClass = 
                    conf.getClass(JobContext.OUTPUT_KEY_CLASS, null);
            if (outputKeyClass == null) {
                job.setOutputKeyClass(DocumentURI.class);
            }
            job.setJobName(getNewJobName(conf));

            AuditUtil.prepareAuditMlcpStart(job, this.name(), cmdline);
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyFilteringConfigOptions(conf, cmdline);
            applyRedactionConfigOptions(conf, cmdline);

            if (cmdline.hasOption(OUTPUT_TYPE)) {
                String outputType = cmdline.getOptionValue(OUTPUT_TYPE);
                conf.set(CONF_OUTPUT_TYPE, outputType);
            }
            if (cmdline.hasOption(OUTPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(OUTPUT_FILE_PATH);
                String wkdir = conf.get(CONF_MAPREDUCE_JOB_WORKING_DIR);
                if (wkdir != null) {
                    path = new Path(wkdir, path).toString();
                }
                conf.set(CONF_OUTPUT_FILEPATH, path);
            }
            if (cmdline.hasOption(OUTPUT_INDENTED)) {
                String isIndented = cmdline.getOptionValue(OUTPUT_INDENTED);
                // check value validity
                if (isIndented != null) {
                    Indentation indent = Indentation.forName(isIndented);
                    conf.set(MarkLogicConstants.INDENTED, indent.name());
                }                
            }
            if (cmdline.hasOption(BASE_PATH)) {
                String basePath = cmdline.getOptionValue(BASE_PATH);
                conf.set(MarkLogicConstants.INPUT_BASE_PATH, basePath);
            }
            if (cmdline.hasOption(API_KEY)) {
                String apiKey = cmdline.getOptionValue(API_KEY);
                conf.set(MarkLogicConstants.INPUT_API_KEY, apiKey);
                // When api key is specified, we are expecting a non-empty
                // base path.
                if (conf.get(MarkLogicConstants.INPUT_BASE_PATH) == null) {
                    throw new IllegalArgumentException("Option " + BASE_PATH +
                        " cannot be empty when api key is specified.");
                }
            }
            String port = DEFAULT_PORT;
            if (cmdline.hasOption(PORT)) {
                port = cmdline.getOptionValue(PORT);
            }
            // If connecting to ML Cloud, ignore port input and use default 443
            if (conf.get(MarkLogicConstants.INPUT_API_KEY) != null) {
                port = DEFAULT_ML_CLOUD_PORT;
            }
            conf.set(MarkLogicConstants.INPUT_PORT, port);
            if (cmdline.hasOption(HOST)) {
                String hosts = cmdline.getOptionValue(HOST);
                InternalUtilities.verifyHosts(
                    hosts, conf.get(MarkLogicConstants.INPUT_PORT));
                conf.set(MarkLogicConstants.INPUT_HOST, hosts);
            }
            if (cmdline.hasOption(RESTRICT_HOSTS)) {
                String restrict = cmdline.getOptionValue(RESTRICT_HOSTS);
                if (isNullOrEqualsTrue(restrict)) {
                    conf.setBoolean(MarkLogicConstants.INPUT_RESTRICT_HOSTS, true);
                    HttpChannel.setUseHTTP(true);
                } else if (!"false".equalsIgnoreCase(restrict)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                                    RESTRICT_INPUT_HOSTS + ": " + restrict);
                }
            } else { // use HTTP compliant mode to true for initial probing
                HttpChannel.setUseHTTP(true);
            }
            // If base path is specified, ignore input from -restrict_hosts and
            // enable restrict hosts and http compliant mode
            if (conf.get(MarkLogicConstants.INPUT_BASE_PATH) != null) {
                conf.setBoolean(MarkLogicConstants.INPUT_RESTRICT_HOSTS, true);
                HttpChannel.setUseHTTP(true);
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
            if (cmdline.hasOption(SSL)) {
                String arg = cmdline.getOptionValue(SSL);
                if (isNullOrEqualsTrue(arg)){
                    conf.setBoolean(MarkLogicConstants.INPUT_USE_SSL, true);
                } else if (!"false".equalsIgnoreCase(arg)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + SSL
                            + ": " + arg);
                } 
            }
            // If connecting to ML Cloud, ignore input from ssl and enable ssl
            if (conf.get(MarkLogicConstants.INPUT_API_KEY) != null) {
                conf.setBoolean(MarkLogicConstants.INPUT_USE_SSL, true);
            }
            applyProtocol(conf, cmdline, SSL_PROTOCOL, MarkLogicConstants.INPUT_SSL_PROTOCOL);
            if (cmdline.hasOption(KEYSTORE_PATH)) {
                String path = cmdline.getOptionValue(KEYSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.INPUT_KEYSTORE_PATH, path);
            }
            if (cmdline.hasOption(KEYSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(KEYSTORE_PASSWD);
                conf.set(MarkLogicConstants.INPUT_KEYSTORE_PASSWD, passwd);
            }
            if (cmdline.hasOption(TRUSTSTORE_PATH)) {
                String path = cmdline.getOptionValue(TRUSTSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.INPUT_TRUSTSTORE_PATH, path);
            }
            if (cmdline.hasOption(TRUSTSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(TRUSTSTORE_PASSWD);
                conf.set(MarkLogicConstants.INPUT_TRUSTSTORE_PASSWD, passwd);
            }

            if (cmdline.hasOption(MAX_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE);
                conf.set(MarkLogicConstants.MAX_SPLIT_SIZE, maxSize);
            }
            if (cmdline.hasOption(CONTENT_ENCODING)) {
                String arg = cmdline.getOptionValue(CONTENT_ENCODING).toUpperCase();
                if ("SYSTEM".equals(arg)) {
                    arg = Charset.defaultCharset().name();
                } else if (!Charset.isSupported(arg)) {
                    throw new IllegalArgumentException(arg
                        + " encoding is not supported");
                }
                conf.set(MarkLogicConstants.OUTPUT_CONTENT_ENCODING, arg);
            }
            // Hidden parameters for MarkLogic Cloud
            if (cmdline.hasOption(TOKEN_ENDPOINT)) {
                String tokenEndpoint = cmdline.getOptionValue(TOKEN_ENDPOINT);
                conf.set(MarkLogicConstants.INPUT_TOKEN_ENDPOINT, tokenEndpoint);
            }
            if (cmdline.hasOption(GRANT_TYPE)) {
                String grantType = cmdline.getOptionValue(GRANT_TYPE);
                conf.set(MarkLogicConstants.INPUT_GRANT_TYPE, grantType);
            }
            if (cmdline.hasOption(TOKEN_DURATION)) {
                String tokenDuration = cmdline.getOptionValue(TOKEN_DURATION);
                conf.set(MarkLogicConstants.INPUT_TOKEN_DURATION, tokenDuration);
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
				Class<? extends Mapper<?,?,?,?>> mapper, int threadsPerSplit) {
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
            configRedactionOptions(options);
            configCopyMLCloudAuthOptions(options);
            
            Option inputUsername = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("User name of the input MarkLogic Server")
                .create(INPUT_USERNAME);
            options.addOption(inputUsername);
            Option inputPassword = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Password of the input MarkLogic Server")
                .create(INPUT_PASSWORD);
            options.addOption(inputPassword);
            Option inputHost = OptionBuilder
                .withArgName("comma list")
                .hasArg()
                .withDescription("Comma-separated list of hosts of the input "
                        + "MarkLogic Server")
                .create(INPUT_HOST);
            inputHost.setRequired(true);
            options.addOption(inputHost);
            Option inputPort = OptionBuilder
                .withArgName("number")
                .hasArg()
                .withDescription("Port of the input MarkLogic Server")
                .create(INPUT_PORT);
            options.addOption(inputPort);
            Option inputDB = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Database of the input MarkLogic Server")
                .create(INPUT_DATABASE);
            options.addOption(inputDB);
            Option restrictInputHosts = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription("Whether to restrict the input hosts mlcp connects to")
                .create(RESTRICT_INPUT_HOSTS);
            options.addOption(restrictInputHosts);
            Option inputSSL = OptionBuilder
                 .withArgName("true,false")
                 .hasOptionalArg()
                 .withDescription(
                 "Use ssl to encrypt communication with input MarkLogic Server")
                 .create(INPUT_SSL);
            options.addOption(inputSSL);

            Option inputSSLProtocol = OptionBuilder
                 .withArgName("string")
                 .hasArg()
                 .withDescription(
                 "Input ssl protocol, e.g. TLS, TLSv1.2")
                 .create(INPUT_SSL_PROTOCOL);
            options.addOption(inputSSLProtocol);

            Option inputKeystorePath = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Input Keystore path to use for SSL connections.")
                    .create(INPUT_KEYSTORE_PATH);
            options.addOption(inputKeystorePath);
            Option inputKeystorePasswd = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Input Keystore password to use for SSL connections")
                    .create(INPUT_KEYSTORE_PASSWD);
            options.addOption(inputKeystorePasswd);
            Option inputTruststorePath = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Input Truststore path to use for SSL connections.")
                    .create(INPUT_TRUSTSTORE_PATH);
            options.addOption(inputTruststorePath);
            Option inputTruststorePasswd = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Input Truststore password to use for SSL connections")
                    .create(INPUT_TRUSTSTORE_PASSWD);
            options.addOption(inputTruststorePasswd);
            Option inputBasePath = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                    "Input base path that maps to a MarkLogic Application Server")
                .create(INPUT_BASE_PATH);
            options.addOption(inputBasePath);
            Option inputApiKey = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                    "API key for the input MarkLogic Cloud")
                .create(INPUT_API_KEY);
            options.addOption(inputApiKey);

            Option outputUsername = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("User Name of the output MarkLogic Server")
                .create(OUTPUT_USERNAME);
            options.addOption(outputUsername);
            Option outputPassword = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Password of the output MarkLogic Server")
                .create(OUTPUT_PASSWORD);
            options.addOption(outputPassword);
            Option outputHost = OptionBuilder
                .withArgName("comma list")
                .hasArg()
                .withDescription("Comma-separated list of hosts of the output "
                        + "MarkLogic Server")
                .create(OUTPUT_HOST);
            outputHost.setRequired(true);
            options.addOption(outputHost);
            Option outputPort = OptionBuilder
                 .withArgName("number")
                 .hasArg()
                 .withDescription("Port of the output MarkLogic Server")
                 .create(OUTPUT_PORT);
            options.addOption(outputPort);
            Option outputDB = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("Database of the output MarkLogic Server")
                .create(OUTPUT_DATABASE);
            options.addOption(outputDB);
            Option restrictOutputHosts = OptionBuilder
                .withArgName("true,fasle")
                .hasOptionalArg()
                .withDescription("Whether to the restrict output hosts mlcp connects to")
                .create(RESTRICT_OUTPUT_HOSTS);
            options.addOption(restrictOutputHosts);
            Option outputSSL = OptionBuilder
                .withArgName("true,false")
                .hasOptionalArg()
                .withDescription(
                "Use ssl to encryt communication with the output MarkLogic Server")
                .create(OUTPUT_SSL);
            options.addOption(outputSSL);

            Option outputSSLProtocol = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                "Output ssl protocol, e.g.TLS, TLSv1")
                .create(OUTPUT_SSL_PROTOCOL);
            options.addOption(outputSSLProtocol);

            Option outputKeystorePath = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Output Keystore path to use for SSL connections.")
                    .create(OUTPUT_KEYSTORE_PATH);
            options.addOption(outputKeystorePath);
            Option outputKeystorePasswd = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Output Keystore password to use for SSL connections")
                    .create(OUTPUT_KEYSTORE_PASSWD);
            options.addOption(outputKeystorePasswd);
            Option outputTruststorePath = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Output Truststore path to use for SSL connections.")
                    .create(OUTPUT_TRUSTSTORE_PATH);
            options.addOption(outputTruststorePath);
            Option outputTruststorePasswd = OptionBuilder
                    .withArgName("string")
                    .hasArg()
                    .withDescription(
                            "Output Truststore password to use for SSL connections")
                    .create(OUTPUT_TRUSTSTORE_PASSWD);
            options.addOption(outputTruststorePasswd);
            Option outputBasePath = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                    "Output base path that maps to a MarkLogic Application Server")
                .create(OUTPUT_BASE_PATH);
            options.addOption(outputBasePath);
            Option outputApiKey = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                    "API key for the output MarkLogic Cloud")
                .create(OUTPUT_API_KEY);
            options.addOption(outputApiKey);
            Option tcf = OptionBuilder
                .withArgName("string")
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
                .withArgName("string")
                .hasArg()
                .withDescription("Output directory in MarkLogic.")
                .create(OUTPUT_DIRECTORY);
            options.addOption(outputDir);
            
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
                if (isNullOrEqualsTrue(arg)){
                    setQueryTimestamp(conf);
                } else if (!"false".equalsIgnoreCase(arg)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + SNAPSHOT
                            + ": " + arg);
                } 
            }
            Job job = LocalJob.getInstance(conf);
            job.setJarByClass(this.getClass());
            Class<?> inputFormatClass = 
                    conf.getClass(JobContext.INPUT_FORMAT_CLASS_ATTR, null);
            if (inputFormatClass == null) {
                job.setInputFormatClass(DatabaseContentInputFormat.class);
            }
            Class<?> mapperClass = 
                    conf.getClass(JobContext.MAP_CLASS_ATTR, null);
            if (mapperClass == null) {  
                job.setMapperClass(DocumentMapper.class);
            }
            Class<?> mapOutputKeyClass = 
                    conf.getClass(JobContext.MAP_OUTPUT_KEY_CLASS, null);
            if (mapOutputKeyClass == null) {
                job.setMapOutputKeyClass(DocumentURI.class);
            }
            Class<?> mapOutputValueClass = 
                    conf.getClass(JobContext.MAP_OUTPUT_VALUE_CLASS, null);
            if (mapOutputValueClass == null) {
                job.setMapOutputValueClass(MarkLogicDocument.class);
            }
            Class<?> outputFormatClass = 
                    conf.getClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR, null);
            if (outputFormatClass == null) {
                if (cmdline.hasOption(TRANSFORM_MODULE)) {
                    job.setOutputFormatClass(
                            DatabaseTransformOutputFormat.class);
                } else {
                    job.setOutputFormatClass(
                            DatabaseContentOutputFormat.class);
                }
            }
            Class<?> outputKeyClass = 
                    conf.getClass(JobContext.OUTPUT_KEY_CLASS, null);
            if (outputKeyClass == null) {
                job.setOutputKeyClass(DocumentURI.class);
            }           
            job.setJobName(getNewJobName(conf));

            AuditUtil.prepareAuditMlcpStart(job, this.name(), cmdline);
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyFilteringConfigOptions(conf, cmdline);
            applyCommonOutputConfigOptions(conf, cmdline);
            applyRedactionConfigOptions(conf, cmdline);

            if (cmdline.hasOption(OUTPUT_USERNAME)) {
                String username = cmdline.getOptionValue(OUTPUT_USERNAME);
                conf.set(MarkLogicConstants.OUTPUT_USERNAME, username);
            }
            if (cmdline.hasOption(OUTPUT_PASSWORD)) {
                String password = cmdline.getOptionValue(OUTPUT_PASSWORD);
                conf.set(MarkLogicConstants.OUTPUT_PASSWORD, password);
            }
            if (cmdline.hasOption(OUTPUT_BASE_PATH)) {
                String outputBasePath = cmdline.getOptionValue(OUTPUT_BASE_PATH);
                conf.set(MarkLogicConstants.OUTPUT_BASE_PATH, outputBasePath);
            }
            if (cmdline.hasOption(OUTPUT_API_KEY)) {
                String outputApiKey = cmdline.getOptionValue(OUTPUT_API_KEY);
                conf.set(MarkLogicConstants.OUTPUT_API_KEY, outputApiKey);
                // When api key is specified, we are expecting a non-empty
                // base path.
                if (conf.get(MarkLogicConstants.OUTPUT_BASE_PATH) == null) {
                    throw new IllegalArgumentException(
                        "Option " + OUTPUT_BASE_PATH +
                        " cannot be empty when output api key is specified.");
                }
            }
            String outputPort = DEFAULT_PORT;
            if (cmdline.hasOption(OUTPUT_PORT)) {
                outputPort = cmdline.getOptionValue(OUTPUT_PORT);
            }
            // If connecting to ML Cloud, ignore port input and use default 443
            if (conf.get(MarkLogicConstants.OUTPUT_API_KEY) != null) {
                outputPort = DEFAULT_ML_CLOUD_PORT;
            }
            conf.set(MarkLogicConstants.OUTPUT_PORT, outputPort);
            if (cmdline.hasOption(OUTPUT_HOST)) {
                String outputHosts = cmdline.getOptionValue(OUTPUT_HOST);
                InternalUtilities.verifyHosts(
                        outputHosts, conf.get(MarkLogicConstants.OUTPUT_PORT));
                conf.set(MarkLogicConstants.OUTPUT_HOST, outputHosts);
            }
            if (cmdline.hasOption(OUTPUT_DATABASE)) {
                String db = cmdline.getOptionValue(OUTPUT_DATABASE);
                conf.set(MarkLogicConstants.OUTPUT_DATABASE_NAME, db);
            }
            if (cmdline.hasOption(RESTRICT_OUTPUT_HOSTS)) {
                String restrict = cmdline.getOptionValue(RESTRICT_OUTPUT_HOSTS);
                if (isNullOrEqualsTrue(restrict)) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_RESTRICT_HOSTS, true);
                    HttpChannel.setUseHTTP(true);
                } else if (!"false".equalsIgnoreCase(restrict)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                                    RESTRICT_OUTPUT_HOSTS + ": " + restrict);
                }
            } else { // use HTTP compliant mode to true for initial probing
                HttpChannel.setUseHTTP(true);
            }
            // If base path is specified, ignore input from -restrict_hosts and
            // enable restrict_hosts and http compliant mode
            if (conf.get(MarkLogicConstants.OUTPUT_BASE_PATH) != null) {
                conf.setBoolean(MarkLogicConstants.OUTPUT_RESTRICT_HOSTS, true);
                HttpChannel.setUseHTTP(true);
            }
            if (cmdline.hasOption(OUTPUT_SSL)) {
                String arg = cmdline.getOptionValue(OUTPUT_SSL);
                if (isNullOrEqualsTrue(arg)){
                    conf.setBoolean(MarkLogicConstants.OUTPUT_USE_SSL, true);
                } else if (!"false".equalsIgnoreCase(arg)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + OUTPUT_SSL
                            + ": " + arg);
                }
            }
            // If api key is specified, ignore input from -ssl and enable ssl
            if (conf.get(MarkLogicConstants.OUTPUT_API_KEY) != null) {
                conf.setBoolean(MarkLogicConstants.OUTPUT_USE_SSL, true);
            }
            applyProtocol(conf, cmdline, OUTPUT_SSL_PROTOCOL, MarkLogicConstants.OUTPUT_SSL_PROTOCOL);
            if (cmdline.hasOption(OUTPUT_KEYSTORE_PATH)) {
                String path = cmdline.getOptionValue(OUTPUT_KEYSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.OUTPUT_KEYSTORE_PATH, path);
            }
            if (cmdline.hasOption(OUTPUT_KEYSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(OUTPUT_KEYSTORE_PASSWD);
                conf.set(MarkLogicConstants.OUTPUT_KEYSTORE_PASSWD, passwd);
            }
            if (cmdline.hasOption(OUTPUT_TRUSTSTORE_PATH)) {
                String path = cmdline.getOptionValue(OUTPUT_TRUSTSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.OUTPUT_TRUSTSTORE_PATH, path);
            }
            if (cmdline.hasOption(OUTPUT_TRUSTSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(OUTPUT_TRUSTSTORE_PASSWD);
                conf.set(MarkLogicConstants.OUTPUT_TRUSTSTORE_PASSWD, passwd);
            }
            if (cmdline.hasOption(OUTPUT_TOKEN_ENDPOINT)) {
                String outputTokenEndpoint = cmdline.getOptionValue(OUTPUT_TOKEN_ENDPOINT);
                conf.set(MarkLogicConstants.OUTPUT_TOKEN_ENDPOINT, outputTokenEndpoint);
            }
            if (cmdline.hasOption(OUTPUT_GRANT_TYPE)) {
                String outputGrantType = cmdline.getOptionValue(OUTPUT_GRANT_TYPE);
                conf.set(MarkLogicConstants.OUTPUT_GRANT_TYPE, outputGrantType);
            }
            if (cmdline.hasOption(OUTPUT_TOKEN_DURATION)) {
                String outputTokenDuration = cmdline.getOptionValue(OUTPUT_TOKEN_DURATION);
                conf.set(MarkLogicConstants.OUTPUT_TOKEN_DURATION, outputTokenDuration);
            }

            if (cmdline.hasOption(INPUT_USERNAME)) {
                String username = cmdline.getOptionValue(INPUT_USERNAME);
                conf.set(MarkLogicConstants.INPUT_USERNAME, username);
            }
            if (cmdline.hasOption(INPUT_PASSWORD)) {
                String password = cmdline.getOptionValue(INPUT_PASSWORD);
                conf.set(MarkLogicConstants.INPUT_PASSWORD, password);
            }
            if (cmdline.hasOption(INPUT_BASE_PATH)) {
                String inputBasePath = cmdline.getOptionValue(INPUT_BASE_PATH);
                conf.set(MarkLogicConstants.INPUT_BASE_PATH, inputBasePath);
            }
            if (cmdline.hasOption(INPUT_API_KEY)) {
                String inputApiKey = cmdline.getOptionValue(INPUT_API_KEY);
                conf.set(MarkLogicConstants.INPUT_API_KEY, inputApiKey);
                // When api key is specified, we are expecting a non-empty
                // base path.
                if (conf.get(MarkLogicConstants.INPUT_BASE_PATH) == null) {
                    throw new IllegalArgumentException(
                        "Option " + INPUT_BASE_PATH +
                            " cannot be empty when input api key is specified.");
                }
            }
            String inputPort = DEFAULT_PORT;
            if (cmdline.hasOption(INPUT_PORT)) {
                inputPort = cmdline.getOptionValue(INPUT_PORT);
            }
            // If connecting to ML Cloud, ignore port input and use default 443
            if (conf.get(MarkLogicConstants.INPUT_API_KEY) != null) {
                inputPort = DEFAULT_ML_CLOUD_PORT;
            }
            conf.set(MarkLogicConstants.INPUT_PORT, inputPort);
            if (cmdline.hasOption(INPUT_HOST)) {
                String inputHosts = cmdline.getOptionValue(INPUT_HOST);
                InternalUtilities.verifyHosts(
                        inputHosts, conf.get(MarkLogicConstants.INPUT_PORT));
                conf.set(MarkLogicConstants.INPUT_HOST, inputHosts);
            }
            if (cmdline.hasOption(INPUT_DATABASE)) {
                String db = cmdline.getOptionValue(INPUT_DATABASE);
                conf.set(MarkLogicConstants.INPUT_DATABASE_NAME, db);
            }
            if (cmdline.hasOption(RESTRICT_INPUT_HOSTS)) {
                String restrict = cmdline.getOptionValue(RESTRICT_INPUT_HOSTS);
                if (isNullOrEqualsTrue(restrict)) {
                    conf.setBoolean(MarkLogicConstants.INPUT_RESTRICT_HOSTS, true);
                    HttpChannel.setUseHTTP(true);
                } else if (!"false".equalsIgnoreCase(restrict)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + 
                                    RESTRICT_INPUT_HOSTS + ": " + restrict);
                }
            } else { // use HTTP compliant mode to true for initial probing
                HttpChannel.setUseHTTP(true);
            }
            // If base path is specified, ignore input from -restrict_hosts and
            // enable restrict_hosts and http compliant mode
            if (conf.get(MarkLogicConstants.INPUT_BASE_PATH) != null) {
                conf.setBoolean(MarkLogicConstants.INPUT_RESTRICT_HOSTS, true);
                HttpChannel.setUseHTTP(true);
            }
            if (cmdline.hasOption(INPUT_SSL)) {
                String arg = cmdline.getOptionValue(INPUT_SSL);
                if (isNullOrEqualsTrue(arg)){
                    conf.setBoolean(MarkLogicConstants.INPUT_USE_SSL, true);
                } else if (!"false".equalsIgnoreCase(arg)) {
                    throw new IllegalArgumentException(
                            "Unrecognized option argument for " + INPUT_SSL
                            + ": " + arg);
                }
            }
            // If api key is specified, ignore input from -ssl and enable ssl
            if (conf.get(MarkLogicConstants.INPUT_API_KEY) != null) {
                conf.setBoolean(MarkLogicConstants.INPUT_USE_SSL, true);
            }
            applyProtocol(conf, cmdline, INPUT_SSL_PROTOCOL, MarkLogicConstants.INPUT_SSL_PROTOCOL);
            if (cmdline.hasOption(INPUT_KEYSTORE_PATH)) {
                String path = cmdline.getOptionValue(INPUT_KEYSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.INPUT_KEYSTORE_PATH, path);
            }
            if (cmdline.hasOption(INPUT_KEYSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(INPUT_KEYSTORE_PASSWD);
                conf.set(MarkLogicConstants.INPUT_KEYSTORE_PASSWD, passwd);
            }
            if (cmdline.hasOption(INPUT_TRUSTSTORE_PATH)) {
                String path = cmdline.getOptionValue(INPUT_TRUSTSTORE_PATH);
                path = new Path(path).toString();
                conf.set(MarkLogicConstants.INPUT_TRUSTSTORE_PATH, path);
            }
            if (cmdline.hasOption(INPUT_TRUSTSTORE_PASSWD)) {
                String passwd = cmdline.getOptionValue(INPUT_TRUSTSTORE_PASSWD);
                conf.set(MarkLogicConstants.INPUT_TRUSTSTORE_PASSWD, passwd);
            }
            if (cmdline.hasOption(INPUT_TOKEN_ENDPOINT)) {
                String inputTokenEndpoint = cmdline.getOptionValue(INPUT_TOKEN_ENDPOINT);
                conf.set(MarkLogicConstants.INPUT_TOKEN_ENDPOINT, inputTokenEndpoint);
            }
            if (cmdline.hasOption(INPUT_GRANT_TYPE)) {
                String inputGrantType = cmdline.getOptionValue(INPUT_GRANT_TYPE);
                conf.set(MarkLogicConstants.INPUT_GRANT_TYPE, inputGrantType);
            }
            if (cmdline.hasOption(INPUT_TOKEN_DURATION)) {
                String inputTokenDuration = cmdline.getOptionValue(INPUT_TOKEN_DURATION);
                conf.set(MarkLogicConstants.INPUT_TOKEN_DURATION, inputTokenDuration);
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
                conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, isNullOrEqualsTrue(arg));
            }
            if (cmdline.hasOption(OUTPUT_DIRECTORY)) {
                String outDir = cmdline.getOptionValue(OUTPUT_DIRECTORY);
                conf.set(MarkLogicConstants.OUTPUT_DIRECTORY, outDir);
            }
            if (cmdline.hasOption(TEMPORAL_COLLECTION)) {
                InputType inputType = getInputType(cmdline);   
                String fileType = cmdline.getOptionValue(INPUT_FILE_TYPE);
                ContentType contentType = inputType.getContentType(cmdline);
                if (fileType != null &&
                    InputType.RDF.toString().equalsIgnoreCase(fileType)) {
                    throw new IllegalArgumentException(
                        "Cannot ingest RDF into temporal collection");
                }
                if (contentType != null && ContentType.BINARY == contentType) {
                    throw new IllegalArgumentException(
    		        "Cannot ingest BINARY into temporal collection");
                }
            }
            
            applyPartitionConfigOptions(conf, cmdline);
            
            applyModuleConfigOptions(conf, cmdline);
            applyBatchTxn(conf, cmdline, MAX_BATCH_SIZE);
        }

        @Override
        public void setMapperClass(Job job, Configuration conf,
                                       CommandLine cmdline) {
            job.setMapperClass(DocumentMapper.class);           
        }

        @Override
        public Class<? extends Mapper<?,?,?,?>> getRuntimeMapperClass(
            Job job, Class<? extends Mapper<?,?,?,?>> mapper,
            int threadsPerSplit) {
            return mapper;
        }
    },
    EXTRACT {
        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {  
            if (cmdline.hasOption(OUTPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(OUTPUT_FILE_PATH);
                String wkdir = conf.get(CONF_MAPREDUCE_JOB_WORKING_DIR);
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
                conf.set(MarkLogicConstants.COLLECTION_FILTER, colFilter);
            }
            if (cmdline.hasOption(DIRECTORY_FILTER)) {
                String dirFilter = cmdline.getOptionValue(DIRECTORY_FILTER);
                conf.set(MarkLogicConstants.DIRECTORY_FILTER, dirFilter);
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
                .withArgName("string")
                .hasArg()
                .withDescription("The file system location for input, as a "
                    + "regular expression")
                .create(INPUT_FILE_PATH);
            inputFilePath.setRequired(true);
            options.addOption(inputFilePath);
            Option outputFilePath = OptionBuilder
                .withArgName("string")
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
                .withArgName("comma list")
                .hasArg()
                .withDescription("Comma-separated list of directories")
                .create(DIRECTORY_FILTER);
            options.addOption(df);
            Option cf = OptionBuilder
                .withArgName("comma list")
                .hasArg()
                .withDescription("Comma-separated list of collections")
                .create(COLLECTION_FILTER);
            options.addOption(cf);
            Option tf = OptionBuilder
                .withArgName("comma list")
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
            Job job = LocalJob.getInstance(conf);
            job.setInputFormatClass(ForestInputFormat.class);
            Class<? extends OutputFormat> outputFormatClass = 
                    Command.isOutputCompressed(cmdline) ?
                        ArchiveOutputFormat.class : 
                        SingleDocumentOutputFormat.class;
            job.setOutputFormatClass(outputFormatClass);

            setMapperClass(job, conf, cmdline);
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(ForestDocument.class);
            job.setJobName(getNewJobName(conf));
            
            if (cmdline.hasOption(INPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(INPUT_FILE_PATH);
                FileInputFormat.setInputPaths(job, path);
            }
            
            return job;
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getRuntimeMapperClass(
                Job job, Class<? extends Mapper<?, ?, ?, ?>> mapper,
                int threadsPerSplit) {
            return mapper;
        }

        @Override
        public void setMapperClass(Job job, Configuration conf,
                CommandLine cmdline) {
            job.setMapperClass(DocumentMapper.class);           
        }
    };

    public static final Log LOG = LogFactory.getLog(LocalJobRunner.class);
    private static int jobid = 0;
    private static Random rand = new Random();
    
    public static Command forName(String cmd) {
        if (IMPORT.name().equalsIgnoreCase(cmd)) {
            return IMPORT;
        } else if (EXPORT.name().equalsIgnoreCase(cmd)) {
            return EXPORT;
        } else if (COPY.name().equalsIgnoreCase(cmd)) {
            return COPY;
        } else if (EXTRACT.name().equalsIgnoreCase(cmd)) {
            return EXTRACT;
        } else {
            throw new IllegalArgumentException("Unknown command: " + cmd);
        }
    }
    
    protected static boolean isInputCompressed(CommandLine cmdline) {
        if (cmdline.hasOption(INPUT_COMPRESSED)) {
            String isCompress = cmdline.getOptionValue(INPUT_COMPRESSED);
            return isNullOrEqualsTrue(isCompress);
        }
        return false;
    }
    
    protected static boolean isOutputCompressed(CommandLine cmdline) {
        if (cmdline.hasOption(OUTPUT_COMPRESS)) {
            String isCompress = cmdline.getOptionValue(OUTPUT_COMPRESS);
            return isNullOrEqualsTrue(isCompress);
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
            if (isNullOrEqualsTrue(arg)) {
                InputType inputType = getInputType(cmdline);
                if (inputType != InputType.DOCUMENTS) {
                    LOG.warn("Streaming option is not applicable to input " +
                            "type " + inputType);
                    conf.setBoolean(MarkLogicConstants.OUTPUT_STREAMING, false);
                    return false;
                } else {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_STREAMING, true);
                    return true;
                }  
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
     * @param cmdline command line options
     * @return a Hadoop job
     * @throws IOException
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
     * @param cmdline command line options
     */
    public abstract void setMapperClass(Job job, Configuration conf, 
    		CommandLine cmdline);
    
    public abstract Class<? extends Mapper<?,?,?,?>> getRuntimeMapperClass(
        Job job, Class<? extends Mapper<?,?,?,?>> mapper, int threadsPerSplit);

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
    
    static void configRedactionOptions(Options options) {
    	Option redaction = OptionBuilder
            	.withArgName("comma list")
            	.hasArg()
            	.withDescription("Comma separated list of redaction rule collection URIs")
            	.create(REDACTION);
    	options.addOption(redaction);
    }
    
    static void configCommonOptions(Options options) {
        Option mode = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("Whether to run in local or distributed mode.")
            .create(MODE);
        options.addOption(mode);
        Option hadoopConfDir = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("Override $HADOOP_CONF_DIR")
            .create(HADOOP_CONF_DIR);
        options.addOption(hadoopConfDir);
        Option threadCount = OptionBuilder
            .withArgName("number")
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

    static void configRDFGraphOutputOptions(Options options) {
        Option outputGraph = OptionBuilder.withArgName("string").hasArg()
            .withDescription("Default graph for quad").create(OUTPUT_GRAPH);
        options.addOption(outputGraph);

        Option outputOverrideGraph = OptionBuilder.withArgName("string")
            .hasArg().withDescription("Graph overrided for quad")
            .create(OUTPUT_OVERRIDE_GRAPH);
        options.addOption(outputOverrideGraph);
    }
    
    static void configCommonOutputOptions(Options options) {
        Option outputUriReplace = OptionBuilder
            .withArgName("comma list")
            .hasArg()
            .withDescription("Comma separated list of regex pattern and "
                    + "string pairs, 1st to match a uri segment, 2nd the "
                    + "string to replace with, with the 2nd one in ''")
            .create(OUTPUT_URI_REPLACE);
        options.addOption(outputUriReplace);
        Option outputUriPrefix = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("String to prepend to all document URIs")
            .create(OUTPUT_URI_PREFIX);
        options.addOption(outputUriPrefix);
        Option outputUriSuffix = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("String to append to all document URIs")
            .create(OUTPUT_URI_SUFFIX);
        options.addOption(outputUriSuffix);

        Option outputCollections = OptionBuilder
            .withArgName("comma list")
            .hasArg()
            .withDescription("Comma separated list of collection to be applied"
                    + " to output documents")
            .create(OUTPUT_COLLECTIONS);
        options.addOption(outputCollections);
        
        Option outputPermissions = OptionBuilder
            .withArgName("comma list")
            .hasArg()
            .withDescription("Comma separated list of user-privilege pairs to "
                    + "be applied to output documents")
            .create(OUTPUT_PERMISSIONS);
        options.addOption(outputPermissions);
        Option outputQuantity = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription("Quality to be applied to output documents")
            .create(OUTPUT_QUALITY);
        options.addOption(outputQuantity);
    }

    static void configConnectionId(Options options) {
        Option username = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("User name of MarkLogic Server")
            .create(USERNAME);
        options.addOption(username);
        Option password = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("Password of MarkLogic Server")
            .create(PASSWORD);
        options.addOption(password);
        Option host = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("Comma-separated list of hosts of MarkLogic Server")
            .create(HOST);
        host.setRequired(true);
        options.addOption(host);
        Option port = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription("Port of MarkLogic Server")
            .create(PORT);
        options.addOption(port);
        Option db = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("Database of MarkLogic Server")
            .create(DATABASE);
        options.addOption(db);
        Option restricHosts = OptionBuilder
            .withArgName("true,false")
            .hasOptionalArg()
            .withDescription("Whether to restrict the hosts mlcp connects to")
            .create(RESTRICT_HOSTS);
        options.addOption(restricHosts);
        Option ssl = OptionBuilder
            .withArgName("true,false")
            .hasOptionalArg()
            .withDescription("Use SSL for encryted communication")
            .create(SSL);
        options.addOption(ssl);
        Option ssl_protocol = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("SSL protocol, e.g. TLS, TLSv1")
            .create(SSL_PROTOCOL);
        options.addOption(ssl_protocol);
        Option keystorePath = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                        "Output Keystore path to use for SSL connections.")
                .create(KEYSTORE_PATH);
        options.addOption(keystorePath);
        Option keystorePasswd = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                        "Keystore password to use for SSL connections")
                .create(KEYSTORE_PASSWD);
        options.addOption(keystorePasswd);
        Option truststorePath = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                        "Truststore path to use for SSL connections.")
                .create(TRUSTSTORE_PATH);
        options.addOption(truststorePath);
        Option truststorePasswd = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription(
                        "Truststore password to use for SSL connections")
                .create(TRUSTSTORE_PASSWD);
        options.addOption(truststorePasswd);
        Option basePath = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Base path that maps to a MarkLogic Application Server")
            .create(BASE_PATH);
        options.addOption(basePath);
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
        Option cpmeta = OptionBuilder
            .withArgName("true,false")
            .hasOptionalArg()
            .withDescription("Whether to copy document metadata from source"
                    + " to destination")
            .create(COPY_METADATA);
            options.addOption(cpmeta);
    }

    static void configBatchTxn(Options options) {
        Option batchSize = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription(
                    "Number of documents in one request (default 100)")
            .create(BATCH_SIZE);
        options.addOption(batchSize);
        Option txnSize = OptionBuilder
            .withArgName("number")
            .hasArg()
            .withDescription(
                    "Number of requests in one transaction (default 1)")
            .create(TRANSACTION_SIZE);
        options.addOption(txnSize);
    }
    
    static void configModule(Options options) {
        Option moduleUri = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Path to the module containing the transform function")
            .create(TRANSFORM_MODULE);
        options.addOption(moduleUri);
        Option ns = OptionBuilder.withArgName("string").hasArg()
            .withDescription("Namespace of the transform function")
            .create(TRANSFORM_NAMESPACE);
        options.addOption(ns);
        Option func = OptionBuilder.withArgName("string").hasArg()
            .withDescription("Name of the transform function")
            .create(TRANSFORM_FUNCTION);
        options.addOption(func);
        Option param = OptionBuilder.withArgName("string").hasArg()
            .withDescription("Parameters of the transform function")
            .create(TRANSFORM_PARAM);
        options.addOption(param);
        Option modules = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("The database that contains application modules")
                .create(MODULES);
        options.addOption(modules);
        Option root = OptionBuilder
                .withArgName("string")
                .hasArg()
                .withDescription("The root document directory pathname")
                .create(MODULES_ROOT);
        options.addOption(root);
    }
    
    static void configPartition(Options options) {
        Option partition = OptionBuilder.withArgName("string")
            .hasArg().withDescription("The partition where docs are inserted")
            .create(OUTPUT_PARTITION);
        options.addOption(partition);
    }

    static void configFilteringOptions(Options options) {
        Option df = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("Comma-separated list of directories")
            .create(DIRECTORY_FILTER);
        options.addOption(df);
        Option cf = OptionBuilder
            .withArgName("comma list")
            .hasArg()
            .withDescription("Comma-separated list of collections")
            .create(COLLECTION_FILTER);
        options.addOption(cf);
        Option ds = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("Path expression used to retrieve documents or " +
                    "element nodes from the server")
            .create(DOCUMENT_SELECTOR);
        options.addOption(ds);
        Option ns = OptionBuilder
            .withArgName("comma list")
        	.hasArg()
        	.withDescription("Comma-separated list of alias-URI bindings " +
                    "used in document_selector")
            .create(PATH_NAMESPACE);
        options.addOption(ns);
        Option qf = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription("cts query to retrieve documents with")
            .create(QUERY_FILTER);
        options.addOption(qf);
    }

    static void configMLCloudAuthOptions(Options options) {
        Option apiKey = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "A unique key assigned to a MarkLogic Cloud user")
            .create(API_KEY);
        options.addOption(apiKey);
        // Hidden parameters for MarkLogic Cloud
        Option tokenEndpoint = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "MarkLogic Cloud token endpoint for obtaining session tokens")
            .create(TOKEN_ENDPOINT);
        options.addOption(tokenEndpoint);
        Option grantType = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Methods through which applications can gain session tokens")
            .create(GRANT_TYPE);
        options.addOption(grantType);
        Option tokenDuration = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Lifetime (in minutes) of session tokens")
            .create(TOKEN_DURATION);
        options.addOption(tokenDuration);
    }

    static void configCopyMLCloudAuthOptions(Options options) {
        Option inputApiKey = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "A unique key assigned to a input MarkLogic Cloud user")
            .create(INPUT_API_KEY);
        options.addOption(inputApiKey);
        Option outputApiKey = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "A unique key assigned to an output MarkLogic Cloud user")
            .create(OUTPUT_API_KEY);
        options.addOption(outputApiKey);
        // Hidden parameters for MarkLogic Cloud
        Option inputTokenEndpoint = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Input MarkLogic Cloud token endpoint for obtaining session tokens")
            .create(INPUT_TOKEN_ENDPOINT);
        options.addOption(inputTokenEndpoint);
        Option outputTokenEndpoint = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Output MarkLogic Cloud token endpoint for obtaining session tokens")
            .create(OUTPUT_TOKEN_ENDPOINT);
        options.addOption(outputTokenEndpoint);
        Option inputGrantType = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Input methods through which applications can gain access tokens")
            .create(INPUT_GRANT_TYPE);
        options.addOption(inputGrantType);
        Option outputGrantType = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Output methods through which applications can gain session tokens")
            .create(OUTPUT_GRANT_TYPE);
        options.addOption(outputGrantType);
        Option inputTokenDuration = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Lifetime (in minutes) of input session tokens")
            .create(INPUT_TOKEN_DURATION);
        options.addOption(inputTokenDuration);
        Option outputTokenDuration = OptionBuilder
            .withArgName("string")
            .hasArg()
            .withDescription(
                "Lifetime (in minutes) of output session tokens")
            .create(OUTPUT_TOKEN_DURATION);
        options.addOption(outputTokenDuration);
    }
    
    static void applyModuleConfigOptions(Configuration conf,
        CommandLine cmdline) {
        if (cmdline.hasOption(TRANSFORM_MODULE)) {
            if (conf.getBoolean(MarkLogicConstants.OUTPUT_STREAMING, false)) {
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
            if (cmdline.hasOption(MODULES)){
                arg = cmdline.getOptionValue(MODULES);
                conf.set(CONF_INPUT_MODULES_DATABASE, arg);
            }
            if (cmdline.hasOption(MODULES_ROOT)){
                arg = cmdline.getOptionValue(MODULES_ROOT);
                conf.set(CONF_INPUT_MODULES_ROOT, arg);
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
            if (isNullOrEqualsTrue(arg)) {
                conf.setBoolean(MarkLogicConstants.COPY_COLLECTIONS, true);
            } else if ("false".equalsIgnoreCase(arg)) {
                conf.setBoolean(MarkLogicConstants.COPY_COLLECTIONS, false);
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized option argument for " + COPY_COLLECTIONS
                                + ": " + arg);
            }
        } else {
            conf.set(MarkLogicConstants.COPY_COLLECTIONS, 
                    DEFAULT_COPY_COLLECTIONS);
        }
        if (cmdline.hasOption(COPY_PERMISSIONS)) {
            String arg = cmdline.getOptionValue(COPY_PERMISSIONS);
            if (isNullOrEqualsTrue(arg)) {
                conf.setBoolean(CONF_COPY_PERMISSIONS, true);
            } else if ("false".equalsIgnoreCase(arg)) {
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
            conf.setBoolean(CONF_COPY_PROPERTIES, isNullOrEqualsTrue(arg));
        } else {
            conf.set(CONF_COPY_PROPERTIES, DEFAULT_COPY_PROPERTIES);
        }
        if (cmdline.hasOption(COPY_QUALITY)) {
            String arg = cmdline.getOptionValue(COPY_QUALITY);
            if (isNullOrEqualsTrue(arg)) {
                conf.setBoolean(MarkLogicConstants.COPY_QUALITY, true);
            } else if ("false".equalsIgnoreCase(arg)) {
                conf.setBoolean(MarkLogicConstants.COPY_QUALITY, false);
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized option argument for " + COPY_QUALITY
                                + ": " + arg);
            }
        } else {
            conf.set(MarkLogicConstants.COPY_QUALITY, DEFAULT_COPY_QUALITY);
        }
        if (cmdline.hasOption(COPY_METADATA)) {
            String arg = cmdline.getOptionValue(COPY_METADATA);
            if (isNullOrEqualsTrue(arg)) {
                conf.setBoolean(MarkLogicConstants.COPY_METADATA, true);
            } else if ("false".equalsIgnoreCase(arg)) {
                conf.setBoolean(MarkLogicConstants.COPY_METADATA, false);
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized option argument for " + COPY_METADATA
                                + ": " + arg);
            }
        } else {
            conf.set(MarkLogicConstants.COPY_METADATA, DEFAULT_COPY_METADATA);
        }
    }

    static void applyFilteringConfigOptions(Configuration conf,
                    CommandLine cmdline) {
        int filters = cmdline.hasOption(COLLECTION_FILTER) ? 1 : 0;
        filters += cmdline.hasOption(DIRECTORY_FILTER) ? 1 : 0;
        filters += cmdline.hasOption(DOCUMENT_SELECTOR) ? 1 : 0;
        if (filters > 1) {
            LOG.error("Only one of " + COLLECTION_FILTER + ", " +
                    DIRECTORY_FILTER + " and " +
                    DOCUMENT_SELECTOR +
                    " can be specified.");
            throw new IllegalArgumentException(
                    "Only one of " + COLLECTION_FILTER + ", " +
                    DIRECTORY_FILTER + ", " + QUERY_FILTER + " and " +
                    DOCUMENT_SELECTOR +
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
                conf.set(MarkLogicConstants.COLLECTION_FILTER, sb.toString());
                conf.set(MarkLogicConstants.DOCUMENT_SELECTOR,
                    "fn:collection(" + sb.toString() + ")");
            } else {
                conf.set(MarkLogicConstants.COLLECTION_FILTER, "\"" + c + "\"");
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
                conf.set(MarkLogicConstants.DIRECTORY_FILTER, sb.toString());
                conf.set(MarkLogicConstants.DOCUMENT_SELECTOR,
                    "xdmp:directory(" + sb.toString() + ",\"infinity\")");
            } else {
                if (!d.endsWith("/")) {
                    LOG.warn("directory_filter: Directory does not end "
                        + "with a forward slash (/): " + d);
                }
                conf.set(MarkLogicConstants.DIRECTORY_FILTER, "\"" + d
                    + "\"");
                conf.set(MarkLogicConstants.DOCUMENT_SELECTOR,
                    "xdmp:directory(\"" + d + "\",\"infinity\")");
            }
        }
        if (cmdline.hasOption(DOCUMENT_SELECTOR)) {
            conf.set(MarkLogicConstants.DOCUMENT_SELECTOR, 
                    cmdline.getOptionValue(DOCUMENT_SELECTOR));
        }
        if (cmdline.hasOption(PATH_NAMESPACE)) {
            conf.set(MarkLogicConstants.PATH_NAMESPACE, 
                    cmdline.getOptionValue(PATH_NAMESPACE));
        }
        if (cmdline.hasOption(QUERY_FILTER)) {
            conf.set(MarkLogicConstants.QUERY_FILTER, 
                    cmdline.getOptionValue(QUERY_FILTER));
        }
    }
    
    static void applyBatchTxn(Configuration conf, CommandLine cmdline, 
            int maxBatch) {
        String batchSize = cmdline.getOptionValue(BATCH_SIZE);
        int batch;
        if (batchSize != null) {
            batch = Integer.decode(batchSize);
            if (batch > maxBatch) {
                LOG.warn("The setting for " + BATCH_SIZE + 
                        " is changed to " + maxBatch);
                batch = maxBatch;
            }
        } else {
            batch = MarkLogicConstants.DEFAULT_BATCH_SIZE > maxBatch ?
                    maxBatch : MarkLogicConstants.DEFAULT_BATCH_SIZE;
        }
        conf.setInt(MarkLogicConstants.BATCH_SIZE, batch);

        String txnSize = cmdline.getOptionValue(TRANSACTION_SIZE);
        if (txnSize != null) {
            int txn = Integer.decode(txnSize);
            if (txn * batch > MAX_TXN_SIZE) {
                txn = MAX_TXN_SIZE / batch;
                LOG.warn("The setting for " + TRANSACTION_SIZE + 
                        " is changed to " + txn);
            }
            conf.setInt(MarkLogicConstants.TXN_SIZE, txn);
        }
    }

    static void applyRDFGraphOutputConfigOptions(Configuration conf,
        CommandLine cmdline) {
        if (cmdline.hasOption(OUTPUT_GRAPH) && cmdline.hasOption(OUTPUT_OVERRIDE_GRAPH)) {
            throw new IllegalArgumentException(
                "Only one of " + OUTPUT_GRAPH + ", " +
                OUTPUT_OVERRIDE_GRAPH + " can be specified.");
        }
        if (cmdline.hasOption(OUTPUT_GRAPH)) {
            String graph = cmdline.getOptionValue(OUTPUT_GRAPH);
            conf.set(MarkLogicConstants.OUTPUT_GRAPH, graph);
        }
        if (cmdline.hasOption(OUTPUT_OVERRIDE_GRAPH)) {
            String graph = cmdline.getOptionValue(OUTPUT_OVERRIDE_GRAPH);
            conf.set(MarkLogicConstants.OUTPUT_OVERRIDE_GRAPH, graph);
        }
    }
    
    static void applyRedactionConfigOptions(Configuration conf, 
    		CommandLine cmdline) {
    	if (cmdline.hasOption(REDACTION)) {
    		String value = cmdline.getOptionValue(REDACTION);
            conf.set(MarkLogicConstants.REDACTION_RULE_COLLECTION, value);
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
                conf.setStrings(MarkLogicConstants.OUTPUT_URI_REPLACE, 
                        uriReplace);
            }
        }
        if (cmdline.hasOption(OUTPUT_URI_PREFIX)) {
            String outPrefix = cmdline.getOptionValue(OUTPUT_URI_PREFIX);
            conf.set(MarkLogicConstants.OUTPUT_URI_PREFIX, outPrefix);
        }
        if (cmdline.hasOption(OUTPUT_URI_SUFFIX)) {
            String outSuffix = cmdline.getOptionValue(OUTPUT_URI_SUFFIX);
            conf.set(MarkLogicConstants.OUTPUT_URI_SUFFIX, outSuffix);
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

    static String getNewJobName(Configuration conf) {
        String mode = conf.get(MarkLogicConstants.EXECUTION_MODE,
                MarkLogicConstants.MODE_LOCAL);
        StringBuilder jobNameBuf = new StringBuilder();
        jobNameBuf.append(mode);
        jobNameBuf.append('_');
        jobNameBuf.append(rand.nextInt(Integer.MAX_VALUE));
        jobNameBuf.append('_');
        jobNameBuf.append(++jobid);
        return jobNameBuf.toString();
    }

    static boolean isNullOrEqualsTrue(String arg) {
        return arg == null || "true".equalsIgnoreCase(arg);
    }

    static void applyProtocol(Configuration conf, CommandLine cmdline, String option, String protocolName) {
        if (cmdline.hasOption(option)) {
            String arg = cmdline.getOptionValue(option);
            if ("TLS".equalsIgnoreCase(arg)) {
                conf.set(protocolName, "TLS");
            } else if ("TLSv1".equalsIgnoreCase(arg)) {
                conf.set(protocolName, "TLSv1");
            } else if ("TLSv1.1".equalsIgnoreCase(arg)) {
                conf.set(protocolName, "TLSv1.1");
            } else if ("TLSv1.2".equalsIgnoreCase(arg)) {
                conf.set(protocolName, "TLSv1.2");
            } else {
                throw new IllegalArgumentException(
                    "Unrecognized option argument for " + option
                        + ": " + arg);
            }
        }
    }

    public void printUsage(Command cmd, Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(HelpFormatter.DEFAULT_WIDTH, 
                cmd.name(), null, options, null, true);
    }
}
