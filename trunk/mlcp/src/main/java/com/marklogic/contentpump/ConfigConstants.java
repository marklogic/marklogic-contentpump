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

/**
 * Constants for configuration option names and values.
 * 
 * @author jchen
 * 
 */
public interface ConfigConstants {
    // property names
    static final String HADOOP_CONFDIR_ENV_NAME = "HADOOP_CONF_DIR";
    static final String CONTENTPUMP_HOME_PROPERTY_NAME = "CONTENTPUMP_HOME";
    static final String CONTENTPUMP_JAR_PREFIX = "mlcp-Hadoop";

    // common
    static final String MODE = "mode";
    static final String MODE_DISTRIBUTED = "distributed";
    static final String MODE_LOCAL = "local";

    static final String HADOOP_CONF_DIR = "hadoop_conf_dir";
    static final String THREAD_COUNT = "thread_count";
    static final String MAX_SPLIT_SIZE = "max_split_size";
    static final int DEFAULT_MAX_SPLIT_SIZE = 50000;
    static final String MIN_SPLIT_SIZE = "min_split_size";
    static final String OPTIONS_FILE = "-options_file";

    // command-specific
    static final String INPUT_FILE_PATH = "input_file_path";
    static final String INPUT_FILE_PATTERN = "input_file_pattern";
    static final String AGGREGATE_RECORD_ELEMENT = "aggregate_record_element";
    static final String AGGREGATE_RECORD_NAMESPACE = "aggregate_record_namespace";
    static final String AGGREGATE_URI_ID = "aggregate_uri_id";
    static final String INPUT_FILE_TYPE = "input_file_type";
    static final String INPUT_FILE_TYPE_DEFAULT = "documents";
    static final String ARCHIVE_METADATA_OPTIONAL = "archive_metadata_optional";
    static final String DEFAULT_ARCHIVE_METADATA_OPTIONAL = "false";
    static final String INPUT_COMPRESSED = "input_compressed";
    static final String INPUT_COMPRESSION_CODEC = "input_compression_codec";
    static final String INPUT_SEQUENCEFILE_KEY_CLASS = "sequencefile_key_class";
    static final String INPUT_SEQUENCEFILE_VALUE_CLASS = "sequencefile_value_class";
    static final String INPUT_SEQUENCEFILE_VALUE_TYPE = "sequencefile_value_type";
    static final String DEFAULT_SEQUENCEFILE_VALUE_TYPE = "TEXT";
    static final String USERNAME = "username";
    static final String PASSWORD = "password";
    static final String HOST = "host";
    static final String PORT = "port";
    static final String OUTPUT_DIRECTORY = "output_directory";
    static final String OUTPUT_COLLECTIONS = "output_collections";
    static final String OUTPUT_PERMISSIONS = "output_permissions";
    static final String OUTPUT_QUALITY = "output_quality";
    static final String OUTPUT_CLEANDIR = "output_cleandir";
    static final String BATCH_SIZE = "batch_size";
    static final String TRANSACTION_SIZE = "transaction_size";
    static final String STREAMING = "streaming";
    static final String NAMESPACE = "namespace";
    static final String DOCUMENT_TYPE = "document_type";
    static final String OUTPUT_IDNAME = "output_idname";
    static final String OUTPUT_LANGUAGE = "output_language";
    static final String OUTPUT_URI_REPLACE = "output_uri_replace";
    static final String OUTPUT_URI_PREFIX = "output_uri_prefix";
    static final String OUTPUT_URI_SUFFIX = "output_uri_suffix";
    static final String OUTPUT_FILENAME_AS_COLLECTION = "filename_as_collection";
    static final String XML_REPAIR_LEVEL = "xml_repair_level";
    static final String COPY_COLLECTIONS = "copy_collections";
    static final String COPY_PERMISSIONS = "copy_permissions";
    static final String COPY_PROPERTIES = "copy_properties";
    static final String COPY_QUALITY = "copy_quality";
    static final String DEFAULT_COPY_COLLECTIONS = "true";
    static final String DEFAULT_COPY_PERMISSIONS = "true";
    static final String DEFAULT_COPY_PROPERTIES = "true";
    static final String DEFAULT_COPY_QUALITY = "true";
    static final String COLLECTION_FILTER = "collection_filter";
    static final String DIRECTORY_FILTER = "directory_filter";
    static final String DOCUMENT_SELECTOR = "document_selector";
    static final String SNAPSHOT = "snapshot";
    static final String OUTPUT_TYPE = "output_type";
    static final String DEFAULT_OUTPUT_TYPE = "document";
    static final String OUTPUT_FILE_PATH = "output_file_path";
    static final String OUTPUT_COMPRESS = "compress";
    static final String OUTPUT_INDENTED = "indented";
    static final String INPUT_USERNAME = "input_username";
    static final String INPUT_PASSWORD = "input_password";
    static final String INPUT_HOST = "input_host";
    static final String INPUT_PORT = "input_port";
    static final String OUTPUT_USERNAME = "output_username";
    static final String OUTPUT_PASSWORD = "output_password";
    static final String OUTPUT_HOST = "output_host";
    static final String OUTPUT_PORT = "output_port";
    static final String DELIMITER = "delimiter";
    static final String DEFAULT_DELIMITER = ",";
    // TODO: merge with "aggregate_uri_id" ??
    static final String DELIMITED_URI_ID = "delimited_uri_id";
    static final String DELIMITED_ROOT_NAME = "delimited_root_name";
    static final String DELIMITED_GENERATE_URI = "generate_uri";
    static final String SPLIT_INPUT = "split_input";
    static final String FAST_LOAD = "fastload";
    static final String CONTENT_ENCODING = "content_encoding";
    static final String THREADS_PER_SPLIT = "thread_count_per_split";
    static final String TOLERATE_ERRORS = "tolerate_errors";
    static final String OUTPUT_PARTITION = "output_partition";
    static final String TRANSFORM_MODULE = "transform_module";
    static final String TRANSFORM_NAMESPACE = "transform_namespace";
    static final String TRANSFORM_FUNCTION = "transform_function";
    static final String TRANSFORM_PARAM = "transform_param";

    static final String RDF_STREAMING_MEMORY_THRESHOLD  = "rdf_streaming_memory_threshold";
    static final String RDF_TRIPLES_PER_DOCUMENT  = "rdf_triples_per_document";

    // configurations to be overwritten in hadoop conf
    static final String CONF_INPUT_COMPRESSION_CODEC = 
        "mapreduce.marklogic.input.compressioncodec";
    static final String CONF_MAX_SPLIT_SIZE = "mapred.max.split.size";
    static final String CONF_MIN_SPLIT_SIZE = "mapred.min.split.size";
    static final String CONF_AGGREGATE_URI_ID = 
        "mapreduce.marklogic.aggregate.uriid";
    static final String CONF_AGGREGATE_RECORD_ELEMENT = 
        "mapreduce.marklogic.aggregate.recordelement";
    static final String CONF_AGGREGATE_RECORD_NAMESPACE = 
        "mapreduce.marklogic.aggregate.recordnamespace";
    static final String CONF_DELIMITER = 
        "mapreduce.marklogic.delimited.delimiter";

    static final String CONF_DELIMITED_URI_ID = 
        "mapreduce.marklogic.delimited.uriid";
    static final String CONF_DELIMITED_ROOT_NAME = 
        "mapreduce.marklogic.delimited.rootname";
    static final String CONF_DELIMITED_GENERATE_URI = 
        "mapreduce.marklogic.delimited.generateuri";
    static final String CONF_SPLIT_INPUT = 
        "mapreduce.marklogic.splitinput";
    static final String CONF_OUTPUT_URI_REPLACE = 
        "mapreduce.marklogic.output.urireplace";
    static final String CONF_OUTPUT_URI_PREFIX = 
        "mapreduce.marklogic.output_uriprefix";
    static final String CONF_OUTPUT_URI_SUFFIX = 
        "mapreduce.marklogic.output_urisuffix";
    static final String CONF_OUTPUT_FILEPATH = "mapred.output.dir";
    static final String CONF_INPUT_FILE_PATTERN = 
        "mapreduce.marklogic.input.filepattern";
    static final String CONF_OUTPUT_FILENAME_AS_COLLECTION = 
        "mapreduce.marklogic.output.filenameascollection";
    static final String CONF_MODE = "mapreduce.marklogic.mode";
    static final String CONF_INPUT_SEQUENCEFILE_KEY_CLASS = 
        "mapreduce.marklogic.input.sequencefile.keyclass";
    static final String CONF_INPUT_SEQUENCEFILE_VALUE_CLASS = 
        "mapreduce.marklogic.input.sequencefile.valueclass";
    static final String CONF_INPUT_SEQUENCEFILE_VALUE_TYPE = 
        "mapreduce.marklogic.input.sequencefile.valuetype";
    static final String CONF_DIRECTORY_FILTER = 
        "mapreduce.marklogic.directory.filter";
    static final String CONF_COLLECTION_FILTER = 
        "mapreduce.marklogic.collection.filter";
    static final String CONF_OUTPUT_TYPE = "mapreduce.marklogic.output.type";
    static final String CONF_COPY_COLLECTIONS = 
        "mapreduce.marklogic.copycollections";
    static final String CONF_COPY_PERMISSIONS = 
        "mapreduce.marklogic.copypermissions";
    static final String CONF_COPY_PROPERTIES = 
        "mapreduce.marklogic.copyproperties";
    static final String CONF_COPY_QUALITY = "mapreduce.marklogic.copyquality";
    static final String CONF_INPUT_ARCHIVE_METADATA_OPTIONAL = 
        "mapreduce.marklogic.input.archive.metadataoptional";
    static final String CONF_THREADS_PER_SPLIT = 
        "mapreduce.marklogic.multithreadedmapper.threads";
    static final String CONF_MULTITHREADEDMAPPER_CLASS = 
        "mapreduce.marklogic.multithreadedmapper.class";
    static final String CONF_TRANSFORM_MODULE = "mapreduce.marklogic.transformmodule";
    static final String CONF_TRANSFORM_NAMESPACE = "mapreduce.marklogic.transformnamespace";
    static final String CONF_TRANSFORM_FUNCTION = "mapreduce.marklogic.transformfunction";
    static final String CONF_TRANSFORM_PARAM = "mapreduce.marklogic.transformparam";
    static final String CONF_MIMETYPES = "mapreduce.marklogic.mimetypes";
    static final String CONF_MIN_THREADS = "mapreduce.marklogic.minthreads";
}
