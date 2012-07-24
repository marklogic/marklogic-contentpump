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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.Indentation;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;

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
            configUserPswdHostPort(options);
            configCopyOptions(options);
            configCommonOutputOptions(options);
            configBatchTxn(options);
            
            Option inputFilePath = OptionBuilder
                .withArgName(INPUT_FILE_PATH)
                .hasArg()
                .withDescription(
                    "The file system path in which to look for " + "input")
                .create(INPUT_FILE_PATH);
            options.addOption(inputFilePath);
            Option inputFilePattern = OptionBuilder
                .withArgName(INPUT_FILE_PATTERN)
                .hasArg()
                .withDescription(
                    "Matching regex pattern for files found in "
                        + "the input file path").create(INPUT_FILE_PATH);
            options.addOption(inputFilePattern);
            Option aggregateRecordElement = OptionBuilder
                .withArgName(AGGREGATE_RECORD_ELEMENT)
                .hasArg()
                .withDescription(
                    "Element name in which each document is " + "found")
                .create(AGGREGATE_RECORD_ELEMENT);
            options.addOption(aggregateRecordElement);
            Option aggregateRecordNamespace = OptionBuilder
                .withArgName(AGGREGATE_RECORD_NAMESPACE)
                .hasArg()
                .withDescription(
                    "Element namespace in which each document " + "is found")
                .create(AGGREGATE_RECORD_NAMESPACE);
            options.addOption(aggregateRecordNamespace);
            Option aggregateUriId = OptionBuilder
                .withArgName(AGGREGATE_URI_ID)
                .hasArg()
                .withDescription(
                    "Element namespace in which each document " + "is found")
                .create(AGGREGATE_URI_ID);
            options.addOption(aggregateUriId);
            Option optionalMetadata = OptionBuilder
                .withArgName(INPUT_METADATA_OPTIONAL)
                .hasArg()
                .withDescription(
                    "Whether metadata files are optional in " + "input")
                .create(INPUT_METADATA_OPTIONAL);
            options.addOption(optionalMetadata);
            Option inputFileType = OptionBuilder
                .withArgName(INPUT_FILE_TYPE)
                .hasArg()
                .withDescription(
                    "Type of input file.  Valid choices are: "
                        + "documents, XML aggregates, delimited text, and export "
                        + "archive.").create(INPUT_FILE_TYPE);
            options.addOption(inputFileType);
            Option inputCompressed = OptionBuilder
                .withArgName(INPUT_COMPRESSED).hasOptionalArg()
                .withDescription("Whether the input data is compressed")
                .create(INPUT_COMPRESSED);
            options.addOption(inputCompressed);
            Option inputCompressionCodec = OptionBuilder
                .withArgName(INPUT_COMPRESSION_CODEC).hasArg()
                .withDescription("Codec used for compression")
                .create(INPUT_COMPRESSION_CODEC);
            options.addOption(inputCompressionCodec);
            Option documentType = OptionBuilder
                .withArgName(DOCUMENT_TYPE)
                .hasArg()
                .withDescription(
                    "Type of document content. Valid choices: "
                        + "XML, TEXT, BINARY").create(DOCUMENT_TYPE);
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

            
            Option namespace = OptionBuilder.withArgName(NAMESPACE).hasArg()
                .withDescription("Namespace used for output document.")
                .create(NAMESPACE);
            options.addOption(namespace);
            Option outputLanguage = OptionBuilder.withArgName(OUTPUT_LANGUAGE)
                .hasArg().withDescription("Output language.")
                .create(OUTPUT_LANGUAGE);
            options.addOption(outputLanguage);
            Option pattern = OptionBuilder.withArgName(INPUT_FILE_PATTERN)
                .hasArg().withDescription("Input file pattern.")
                .create(INPUT_FILE_PATTERN);
            options.addOption(pattern);
            Option outputCleanDir = OptionBuilder.withArgName(OUTPUT_CLEANDIR)
                .hasOptionalArg()
                .withDescription("Whether to clean dir before output.")
                .create(OUTPUT_CLEANDIR);
            options.addOption(outputCleanDir);
            Option outputDir = OptionBuilder.withArgName(OUTPUT_DIRECTORY)
                .hasArg().withDescription("Output Directory in MarkLogic.")
                .create(OUTPUT_DIRECTORY);
            options.addOption(outputDir);
            Option outputFilenameCollection = OptionBuilder
                .withArgName(OUTPUT_FILENAME_AS_COLLECTION).hasOptionalArg()
                .withDescription("Filename as collection in output.")
                .create(OUTPUT_FILENAME_AS_COLLECTION);
            options.addOption(outputFilenameCollection);
            Option repairLevel = OptionBuilder.withArgName(XML_REPAIR_LEVEL)
                .hasArg().withDescription("XML repair level.")
                .create(XML_REPAIR_LEVEL);
            options.addOption(repairLevel);
            Option seqKeyClass = OptionBuilder
                .withArgName(INPUT_SEQUENCEFILE_KEY_CLASS).hasArg()
                .withDescription("Sequencefile key class.")
                .create(INPUT_SEQUENCEFILE_KEY_CLASS);
            options.addOption(seqKeyClass);
            Option seqValueClass = OptionBuilder
                .withArgName(INPUT_SEQUENCEFILE_VALUE_CLASS).hasArg()
                .withDescription("Sequencefile value class.")
                .create(INPUT_SEQUENCEFILE_VALUE_CLASS);
            options.addOption(seqValueClass);
            Option seqValueType = OptionBuilder
                .withArgName(INPUT_SEQUENCEFILE_VALUE_TYPE).hasArg()
                .withDescription("Sequencefile value type.")
                .create(INPUT_SEQUENCEFILE_VALUE_TYPE);
            options.addOption(seqValueType);

            Option allowEmptyMeta = OptionBuilder
                .withArgName(INPUT_ARCHIVE_ALLOW_EMPTY_METADATA)
                .hasOptionalArg()
                .withDescription(
                    "Whether to allow empty metadata when importing archive")
                .create(INPUT_ARCHIVE_ALLOW_EMPTY_METADATA);
            options.addOption(allowEmptyMeta);

            Option fastLoad = OptionBuilder
                .withArgName(FAST_LOAD)
                .hasOptionalArg()
                .withDescription(
                    "Whether to use the fast load mode to load content into "
                        + "MarkLogic").create(FAST_LOAD);
            options.addOption(fastLoad);

            // TODO: complete
            // Option streaming = OptionBuilder.withArgName(STREAMING).hasArg()
            // .withDescription("Streaming").create(STREAMING);
            // options.addOption(streaming);
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
            throws IOException {
            applyConfigOptions(conf, cmdline);

            String inputTypeOption = cmdline.getOptionValue(INPUT_FILE_TYPE,
                INPUT_FILE_TYPE_DEFAULT);
            InputType type = InputType.forName(inputTypeOption);
            String documentType = conf.get(MarkLogicConstants.CONTENT_TYPE,
                MarkLogicConstants.DEFAULT_CONTENT_TYPE);
            ContentType contentType = ContentType.forName(documentType);
           
            boolean compressed = false;
            if (cmdline.hasOption(INPUT_COMPRESSED)) {
                String arg = cmdline.getOptionValue(INPUT_COMPRESSED);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    compressed = true;
                }
            }

            // construct a job
            Job job = new Job(conf);
            job.setJarByClass(this.getClass());
            job.setInputFormatClass(type.getInputFormatClass(contentType,
                compressed));

            job.setMapperClass(type.getMapperClass(contentType));
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(type.getOutputValueClass(contentType));
            job.setOutputFormatClass(type.getOutputFormatClass(contentType));

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
            applyBatchTxnConfigOptions(conf, cmdline);
            
            if (cmdline.hasOption(INPUT_ARCHIVE_ALLOW_EMPTY_METADATA)) {
                String arg = cmdline
                    .getOptionValue(INPUT_ARCHIVE_ALLOW_EMPTY_METADATA);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(CONF_INPUT_ARCHIVE_ALLOW_EMPTY_METADATA,
                        true);
                } else if (arg.equalsIgnoreCase("false")) {
                    conf.setBoolean(CONF_INPUT_ARCHIVE_ALLOW_EMPTY_METADATA,
                        false);
                } else {
                    LOG.warn("Unrecognized option argument for " + 
                        INPUT_ARCHIVE_ALLOW_EMPTY_METADATA + ": " + arg);
                }
            }

            String documentType = cmdline.getOptionValue(DOCUMENT_TYPE,
                DEFAULT_DOCUMENT_TYPE);
            conf.set(MarkLogicConstants.CONTENT_TYPE,
                documentType.toUpperCase());
            if (cmdline.hasOption(INPUT_COMPRESSION_CODEC)) {
                String codec = cmdline.getOptionValue(INPUT_COMPRESSION_CODEC);
                conf.set(CONF_INPUT_COMPRESSION_CODEC, codec.toUpperCase());
            }
            if (cmdline.hasOption(MAX_SPLIT_SIZE)) {
                String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE);
                conf.set(ConfigConstants.CONF_MAX_SPLIT_SIZE, maxSize);
            }
            if (cmdline.hasOption(MIN_SPLIT_SIZE)) {
                String minSize = cmdline.getOptionValue(MIN_SPLIT_SIZE);
                conf.set(CONF_MIN_SPLIT_SIZE, minSize);
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
            if (cmdline.hasOption(OUTPUT_FILENAME_AS_COLLECTION)) {
                String arg = cmdline
                    .getOptionValue(OUTPUT_FILENAME_AS_COLLECTION);
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
                } else if (arg.equalsIgnoreCase("false")){
                    conf.setBoolean(MarkLogicConstants.OUTPUT_CLEAN_DIR, false);
                } else {
                    LOG.warn("Unrecognized option argument for " + 
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
                String pattern = cmdline.getOptionValue(INPUT_FILE_PATTERN);
                conf.set(CONF_INPUT_FILE_PATTERN, pattern);
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

            String repairLevel = cmdline.getOptionValue(XML_REPAIR_LEVEL,
                MarkLogicConstants.DEFAULT_OUTPUT_XML_REPAIR_LEVEL);
            conf.set(MarkLogicConstants.OUTPUT_XML_REPAIR_LEVEL,
                repairLevel.toUpperCase());
            // String streaming = cmdline.getOptionValue(STREAMING,
            // DEFAULT_STREAMING);
            // conf.set(CONF_STREAMING, streaming);
            if (cmdline.hasOption(INPUT_SEQUENCEFILE_KEY_CLASS)) {
                String keyClass = cmdline
                    .getOptionValue(INPUT_SEQUENCEFILE_KEY_CLASS);
                conf.set(CONF_INPUT_SEQUENCEFILE_KEY_CLASS, keyClass);
            }
            if (cmdline.hasOption(INPUT_SEQUENCEFILE_VALUE_CLASS)) {
                String valueClass = cmdline
                    .getOptionValue(INPUT_SEQUENCEFILE_VALUE_CLASS);
                conf.set(CONF_INPUT_SEQUENCEFILE_VALUE_CLASS, valueClass);
            }
            if (cmdline.hasOption(INPUT_SEQUENCEFILE_VALUE_TYPE)) {
                String valueType = cmdline.getOptionValue(
                    INPUT_SEQUENCEFILE_VALUE_TYPE,
                    DEFAULT_SEQUENCEFILE_VALUE_TYPE);
                conf.set(CONF_INPUT_SEQUENCEFILE_VALUE_TYPE,
                    valueType.toUpperCase());
                if (valueType
                    .equalsIgnoreCase(SequenceFileValueType.BYTESWRITABLE
                        .toString())) {
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
                } else if (arg.equalsIgnoreCase("false")){
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, false);
                } else {
                    LOG.warn("Unrecognized option argument for " + FAST_LOAD +
                                    ": " + arg);
                }
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
            configUserPswdHostPort(options);
            configCopyOptions(options);
            configDocumentFilteringOptions(options);

            Option outputType = OptionBuilder.withArgName(OUTPUT_TYPE)
                .hasArg().withDescription("export output type")
                .create(OUTPUT_TYPE);
            options.addOption(outputType);
            Option outputFilePath = OptionBuilder
                .withArgName(OUTPUT_FILE_PATH).hasArg()
                .withDescription("export output file path")
                .create(OUTPUT_FILE_PATH);
            options.addOption(outputFilePath);
            Option exportCompress = OptionBuilder.withArgName(OUTPUT_COMPRESS)
                .hasOptionalArg()
                .withDescription("Whether to compress the output document")
                .create(OUTPUT_COMPRESS);
            options.addOption(exportCompress);
            Option exportIndented = OptionBuilder
                 .withArgName(OUTPUT_INDENTED)
                 .hasArg()
                 .withDescription("Whether to format data with indentation")
                 .create(OUTPUT_INDENTED);
            options.addOption(exportIndented);
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
            throws IOException {
            applyConfigOptions(conf, cmdline);

            String type = conf.get(CONF_OUTPUT_TYPE, DEFAULT_OUTPUT_TYPE);
            ExportOutputType outputType = ExportOutputType.valueOf(type
                .toUpperCase());

            if (outputType.equals(ExportOutputType.DOCUMENT)) {
                conf.set(MarkLogicConstants.INPUT_MODE,
                    MarkLogicConstants.BASIC_MODE);
                conf.set(MarkLogicConstants.INPUT_VALUE_CLASS,
                    MarkLogicDocument.class.getCanonicalName());
            } else if (outputType.equals(ExportOutputType.ARCHIVE)) {
                // use basic mode for getSplits; use advanced mode(hardcoded)
                // for record reader
                conf.set(MarkLogicConstants.INPUT_MODE,
                    MarkLogicConstants.BASIC_MODE);
            }

            boolean isCompressed = conf
                .getBoolean(CONF_OUTPUT_COMPRESS, false);
            // construct a job
            Job job = new Job(conf);
            job.setJarByClass(this.getClass());
            job.setInputFormatClass(outputType.getInputFormatClass());

            job.setMapperClass(DocumentMapper.class);
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(MarkLogicDocument.class);
            job.setOutputFormatClass(outputType
                .getOutputFormatClass(isCompressed));
            job.setOutputKeyClass(DocumentURI.class);
            String path = conf.get(ConfigConstants.CONF_OUTPUT_FILEPATH);
            //directory should not exist and it will be created
            FileOutputFormat.setOutputPath(job, new Path(path));
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyDocumentFilteringConfigOptions(conf, cmdline);

            if (cmdline.hasOption(OUTPUT_TYPE)) {
                String outputType = cmdline.getOptionValue(OUTPUT_TYPE);
                conf.set(CONF_OUTPUT_TYPE, outputType);
            }
            if (cmdline.hasOption(OUTPUT_FILE_PATH)) {
                String path = cmdline.getOptionValue(OUTPUT_FILE_PATH);
                conf.set(ConfigConstants.CONF_OUTPUT_FILEPATH, path);
            }
            if (cmdline.hasOption(OUTPUT_COMPRESS)) {
                String isCompress = cmdline.getOptionValue(OUTPUT_COMPRESS);
                if (isCompress == null || isCompress.equalsIgnoreCase("true")) {
                    conf.setBoolean(CONF_OUTPUT_COMPRESS, true);
                } else if (isCompress.equalsIgnoreCase("false")){
                    conf.setBoolean(CONF_OUTPUT_COMPRESS, false);
                } else {
                    LOG.warn("Unrecognized option argument for " + 
                                    OUTPUT_COMPRESS + ": " + isCompress);
                }
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
            String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE, "10000");
            conf.set(MarkLogicConstants.MAX_SPLIT_SIZE, maxSize);
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
            configCopyOptions(options);
            configCommonOutputOptions(options);
            configDocumentFilteringOptions(options);
            configBatchTxn(options);
            
            Option iUsername = OptionBuilder.withArgName(INPUT_USERNAME)
                .hasArg()
                .withDescription("Input username of source ML server ")
                .create(INPUT_USERNAME);
            options.addOption(iUsername);
            Option iPswd = OptionBuilder.withArgName(INPUT_PASSWORD).hasArg()
                .withDescription("Input password of source ML server ")
                .create(INPUT_PASSWORD);
            options.addOption(iPswd);
            Option iHost = OptionBuilder.withArgName(INPUT_HOST).hasArg()
                .withDescription("Input host of source ML server")
                .create(INPUT_HOST);
            options.addOption(iHost);
            Option iPort = OptionBuilder.withArgName(INPUT_PORT).hasArg()
                .withDescription("Input port of source ML server")
                .create(INPUT_PORT);
            options.addOption(iPort);

            Option oUsername = OptionBuilder.withArgName(OUTPUT_USERNAME)
                .hasArg()
                .withDescription("Output username of destination ML server ")
                .create(OUTPUT_USERNAME);
            options.addOption(oUsername);
            Option oPswd = OptionBuilder.withArgName(OUTPUT_PASSWORD).hasArg()
                .withDescription("Output password of destination ML server ")
                .create(OUTPUT_PASSWORD);
            options.addOption(oPswd);
            Option oHost = OptionBuilder.withArgName(OUTPUT_HOST).hasArg()
                .withDescription("Output host of destination ML server")
                .create(OUTPUT_HOST);
            options.addOption(oHost);
            Option oPort = OptionBuilder.withArgName(OUTPUT_PORT).hasArg()
                .withDescription("Output port of destination ML server")
                .create(OUTPUT_PORT);
            options.addOption(oPort);

            Option fastLoad = OptionBuilder
                .withArgName(FAST_LOAD)
                .hasOptionalArg()
                .withDescription(
                    "Whether to use the fast load mode to load content into "
                        + "MarkLogic").create(FAST_LOAD);
            options.addOption(fastLoad);
            
            Option outputDir = OptionBuilder.withArgName(OUTPUT_DIRECTORY)
                .hasArg().withDescription("Output Directory in MarkLogic.")
                .create(OUTPUT_DIRECTORY);
            options.addOption(outputDir);
        }

        @Override
        public Job createJob(Configuration conf, CommandLine cmdline)
            throws IOException {
            applyConfigOptions(conf, cmdline);

            Job job = new Job(conf);
            job.setJarByClass(this.getClass());
            job.setInputFormatClass(MarkLogicDocumentInputFormat.class);
            job.setMapperClass(DocumentMapper.class);
            job.setMapOutputKeyClass(DocumentURI.class);
            job.setMapOutputValueClass(MarkLogicDocument.class);
            job.setOutputFormatClass(ImportArchiveOutputFormat.class);
            job.setOutputKeyClass(DocumentURI.class);
            return job;
        }

        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline) {
            applyCopyConfigOptions(conf, cmdline);
            applyDocumentFilteringConfigOptions(conf, cmdline);
            applyCommonOutputConfigOptions(conf, cmdline);
            applyBatchTxnConfigOptions(conf, cmdline);
            
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
            String maxSize = cmdline.getOptionValue(MAX_SPLIT_SIZE, "10000");
            conf.set(MarkLogicConstants.MAX_SPLIT_SIZE, maxSize);
            if (cmdline.hasOption(FAST_LOAD)) {
                String arg = cmdline.getOptionValue(FAST_LOAD);
                if (arg == null || arg.equalsIgnoreCase("true")) {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, true);
                } else {
                    conf.setBoolean(MarkLogicConstants.OUTPUT_FAST_LOAD, false);
                }
            }
            if (cmdline.hasOption(OUTPUT_DIRECTORY)) {
                String outDir = cmdline.getOptionValue(OUTPUT_DIRECTORY);
                conf.set(MarkLogicConstants.OUTPUT_DIRECTORY, outDir);
            }
        }

        @Override
        public void printUsage() {
            // TODO Auto-generated method stub

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

    static void configCommonOptions(Options options) {
        Option mode = OptionBuilder
            .withArgName(MODE)
            .hasArg()
            .withDescription("Whether to run in single client or distributed.")
            .create(MODE);
        options.addOption(mode);
        Option hadoopHome = OptionBuilder.withArgName(HADOOP_HOME).hasArg()
            .withDescription("Override $HADOOP_HOME").create(HADOOP_HOME);
        options.addOption(hadoopHome);
        Option threadCount = OptionBuilder.withArgName(THREAD_COUNT).hasArg()
            .withDescription("Number of threads").create(THREAD_COUNT);
        options.addOption(threadCount);
        Option maxSplitSize = OptionBuilder.withArgName(MAX_SPLIT_SIZE)
            .hasArg()
            .withDescription("Maximum number of records per each split")
            .create(MAX_SPLIT_SIZE);
        options.addOption(maxSplitSize);
        Option minSplitSize = OptionBuilder.withArgName(MIN_SPLIT_SIZE)
            .hasArg()
            .withDescription("Minimum number of records per each split")
            .create(MIN_SPLIT_SIZE);
        options.addOption(minSplitSize);
    }

    static void configCommonOutputOptions(Options options) {
        Option outputUriReplace = OptionBuilder
            .withArgName(OUTPUT_URI_REPLACE).hasArg()
            .withDescription("Replace URI in output.")
            .create(OUTPUT_URI_REPLACE);
        options.addOption(outputUriReplace);
        Option outputUriPrefix = OptionBuilder.withArgName(OUTPUT_URI_PREFIX)
            .hasArg().withDescription("Prefix added to URI in output.")
            .create(OUTPUT_URI_PREFIX);
        options.addOption(outputUriPrefix);
        Option outputUriSuffix = OptionBuilder.withArgName(OUTPUT_URI_SUFFIX)
            .hasArg().withDescription("Suffix added to URI in output.")
            .create(OUTPUT_URI_SUFFIX);
        options.addOption(outputUriSuffix);

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

    }

    static void configUserPswdHostPort(Options options) {
        Option username = OptionBuilder.withArgName(USERNAME).hasArg()
            .withDescription("Username.").create(USERNAME);
        options.addOption(username);
        Option pswd = OptionBuilder.withArgName(PASSWORD).hasArg()
            .withDescription("Password.").create(PASSWORD);
        options.addOption(pswd);
        Option host = OptionBuilder.withArgName(HOST).hasArg()
            .withDescription("Host.").create(HOST);
        options.addOption(host);
        Option port = OptionBuilder.withArgName(PORT).hasArg()
            .withDescription("Port.").create(PORT);
        options.addOption(port);
    }

    static void configCopyOptions(Options options) {
        Option cpcol = OptionBuilder.withArgName(COPY_COLLECTIONS)
            .hasOptionalArg().withDescription("Copy collections.")
            .create(COPY_COLLECTIONS);
        options.addOption(cpcol);
        Option cppm = OptionBuilder.withArgName(COPY_PERMISSIONS)
            .hasOptionalArg().withDescription("Copy permissions.")
            .create(COPY_PERMISSIONS);
        options.addOption(cppm);
        Option cppt = OptionBuilder.withArgName(COPY_PROPERTIES)
            .hasOptionalArg().withDescription("Copy properties.")
            .create(COPY_PROPERTIES);
        options.addOption(cppt);
        Option cpqt = OptionBuilder.withArgName(COPY_QUALITY).hasOptionalArg()
            .withDescription("Copy quality.").create(COPY_QUALITY);
        options.addOption(cpqt);
    }
    
    static void configBatchTxn(Options options) {
        Option batchSize = OptionBuilder.withArgName(BATCH_SIZE).hasArg()
            .withDescription("Batch size.").create(BATCH_SIZE);
        options.addOption(batchSize);
        Option txnSize = OptionBuilder.withArgName(TRANSACTION_SIZE).hasArg()
            .withDescription("Transaction size.").create(TRANSACTION_SIZE);
        options.addOption(txnSize);
    }
    static void configDocumentFilteringOptions(Options options) {
        Option filter = OptionBuilder.withArgName(DOCUMENT_FILTER).hasArg()
            .withDescription("Path expression used to retrieve records ")
            .create(DOCUMENT_FILTER);
        options.addOption(filter);
        Option ns = OptionBuilder.withArgName(DOCUMENT_NAMESPACE).hasArg()
            .withDescription("Path expression used to retrieve records ")
            .create(DOCUMENT_NAMESPACE);
        options.addOption(ns);
    }

    static void applyCopyConfigOptions(Configuration conf, CommandLine cmdline) {
        if (cmdline.hasOption(COPY_COLLECTIONS)) {
            String arg = cmdline.getOptionValue(COPY_COLLECTIONS);
            if (arg == null || arg.equalsIgnoreCase("true")) {
                conf.setBoolean(CONF_COPY_COLLECTIONS, true);
            } else if (arg.equalsIgnoreCase("false")) {
                conf.setBoolean(CONF_COPY_COLLECTIONS, false);
            } else {
                LOG.warn("Unrecognized option argument for " + COPY_COLLECTIONS
                                + ": " + arg);
                conf.set(CONF_COPY_COLLECTIONS, DEFAULT_COPY_COLLECTIONS);
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
                LOG.warn("Unrecognized option argument for " + COPY_PERMISSIONS
                                + ": " + arg);
                conf.set(CONF_COPY_PERMISSIONS, DEFAULT_COPY_PERMISSIONS);
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
                LOG.warn("Unrecognized option argument for " + COPY_QUALITY
                                + ": " + arg);
                conf.set(CONF_COPY_QUALITY, DEFAULT_COPY_QUALITY);
            }
        } else {
            conf.set(CONF_COPY_QUALITY, DEFAULT_COPY_QUALITY);
        }
    }

    static void applyDocumentFilteringConfigOptions(Configuration conf,
        CommandLine cmdline) {
        String c = cmdline.getOptionValue(DOCUMENT_FILTER,
            DEFAULT_DOCUMENT_FILTER);
        conf.set(MarkLogicConstants.DOCUMENT_SELECTOR, c);
        if (cmdline.hasOption(DOCUMENT_NAMESPACE)) {
            String ns = cmdline.getOptionValue(DOCUMENT_NAMESPACE);
            conf.set(MarkLogicConstants.PATH_NAMESPACE, ns);
        }
    }

    static void applyBatchTxnConfigOptions(Configuration conf,
        CommandLine cmdline) {
        String batchSize = cmdline.getOptionValue(BATCH_SIZE,
            String.valueOf(DEFAULT_BATCH_SIZE));
        conf.set(MarkLogicConstants.BATCH_SIZE, batchSize);
        String txnSize = cmdline.getOptionValue(TRANSACTION_SIZE,
            String.valueOf(DEFAULT_TRANSACTION_SIZE));
        conf.set(MarkLogicConstants.TXN_SIZE, txnSize);
    }
    
    static void applyCommonOutputConfigOptions(Configuration conf,
        CommandLine cmdline) {

        if (cmdline.hasOption(OUTPUT_URI_REPLACE)) {
            String uriReplace = cmdline.getOptionValue(OUTPUT_URI_REPLACE);
            if (uriReplace == null) {
                LOG.error(OUTPUT_URI_REPLACE + " is not configured correctly.");
            } else {
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
            String collectionsString = cmdline
                .getOptionValue(OUTPUT_COLLECTIONS);
            conf.set(MarkLogicConstants.OUTPUT_COLLECTION, collectionsString);
        }
        if (cmdline.hasOption(OUTPUT_PERMISSIONS)) {
            String permissionString = cmdline
                .getOptionValue(OUTPUT_PERMISSIONS);
            conf.set(MarkLogicConstants.OUTPUT_PERMISSION, permissionString);
        }
        if (cmdline.hasOption(OUTPUT_QUALITY)) {
            String quantity = cmdline.getOptionValue(OUTPUT_QUALITY);
            conf.set(MarkLogicConstants.OUTPUT_QUALITY, quantity);
        }
    }

    public abstract void printUsage();
}
