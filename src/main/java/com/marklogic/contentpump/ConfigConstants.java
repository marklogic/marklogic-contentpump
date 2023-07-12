/*
 * Copyright (c) 2020 MarkLogic Corporation
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

import java.util.concurrent.TimeUnit;

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
    static final String CONTENTPUMP_JAR_PREFIX = "mlcp";
    static final String CONTENTPUMP_BUNDLE_ARTIFACT = "BUNDLE_ARTIFACT";

    // common
    static final String MODE = "mode";

    static final String HADOOP_CONF_DIR = "hadoop_conf_dir";
    static final String THREAD_COUNT = "thread_count";
    static final int DEFAULT_THREAD_COUNT = 4;
    static final String MAX_SPLIT_SIZE = "max_split_size";
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
    static final String DEFAULT_PORT = "8000";
    static final String DATABASE = "database";
    static final String OUTPUT_DIRECTORY = "output_directory";
    static final String OUTPUT_COLLECTIONS = "output_collections";
    static final String OUTPUT_GRAPH = "output_graph";
    static final String OUTPUT_OVERRIDE_GRAPH = "output_override_graph";
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
    static final String COPY_METADATA = "copy_metadata";
    static final String DEFAULT_COPY_COLLECTIONS = "true";
    static final String DEFAULT_COPY_PERMISSIONS = "true";
    static final String DEFAULT_COPY_PROPERTIES = "true";
    static final String DEFAULT_COPY_QUALITY = "true";
    static final String DEFAULT_COPY_METADATA = "true";
    static final String COLLECTION_FILTER = "collection_filter";
    static final String DIRECTORY_FILTER = "directory_filter";
    static final String QUERY_FILTER = "query_filter";
    static final String TYPE_FILTER = "type_filter";
    static final String DOCUMENT_SELECTOR = "document_selector";
    static final String PATH_NAMESPACE = "path_namespace";
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
    static final String INPUT_DATABASE = "input_database";
    static final String OUTPUT_USERNAME = "output_username";
    static final String OUTPUT_PASSWORD = "output_password";
    static final String OUTPUT_HOST = "output_host";
    static final String OUTPUT_PORT = "output_port";
    static final String OUTPUT_DATABASE = "output_database";
    static final String DELIMITER = "delimiter";
    static final String DEFAULT_DELIMITER = ",";
    static final String DELIMITED_URI_ID = "delimited_uri_id";
    static final String DELIMITED_ROOT_NAME = "delimited_root_name";
    static final String GENERATE_URI = "generate_uri";
    static final String SPLIT_INPUT = "split_input";
    static final String FAST_LOAD = "fastload";
    static final String CONTENT_ENCODING = "content_encoding";
    static final String THREADS_PER_SPLIT = "thread_count_per_split";
    static final String OUTPUT_PARTITION = "output_partition";
    static final String TRANSFORM_MODULE = "transform_module";
    static final String TRANSFORM_NAMESPACE = "transform_namespace";
    static final String TRANSFORM_FUNCTION = "transform_function";
    static final String TRANSFORM_PARAM = "transform_param";
    static final String TEMPORAL_COLLECTION = "temporal_collection";
    static final String REDACTION = "redaction";
    static final String RESTRICT_HOSTS = "restrict_hosts";
    static final String RESTRICT_INPUT_HOSTS = "restrict_input_hosts";
    static final String RESTRICT_OUTPUT_HOSTS = "restrict_output_hosts";
    static final String SSL = "ssl";
    static final String INPUT_SSL = "input_ssl";
    static final String OUTPUT_SSL = "output_ssl";
    static final String SSL_PROTOCOL = "ssl_protocol";
    static final String INPUT_SSL_PROTOCOL = "input_ssl_protocol";
    static final String OUTPUT_SSL_PROTOCOL = "output_ssl_protocol";
    static final String KEYSTORE_PATH ="keystore_path";
    static final String KEYSTORE_PASSWD ="keystore_password";
    static final String TRUSTSTORE_PATH ="truststore_path";
    static final String TRUSTSTORE_PASSWD ="truststore_password";
    static final String INPUT_KEYSTORE_PATH ="input_keystore_path";
    static final String INPUT_KEYSTORE_PASSWD ="input_keystore_password";
    static final String INPUT_TRUSTSTORE_PATH ="input_truststore_path";
    static final String INPUT_TRUSTSTORE_PASSWD ="input_truststore_password";
    static final String OUTPUT_KEYSTORE_PATH ="output_keystore_path";
    static final String OUTPUT_KEYSTORE_PASSWD ="output_keystore_password";
    static final String OUTPUT_TRUSTSTORE_PATH ="output_truststore_path";
    static final String OUTPUT_TRUSTSTORE_PASSWD ="output_truststore_password";
    static final String MODULES = "modules";
    static final String MODULES_ROOT = "modules_root";
    static final String MAX_THREADS = "max_threads";
    static final String MAX_THREAD_PERCENTAGE = "max_thread_percentage";

    static final String RDF_STREAMING_MEMORY_THRESHOLD  = 
            "rdf_streaming_memory_threshold";
    static final String RDF_TRIPLES_PER_DOCUMENT  = "rdf_triples_per_document";
    static final String DEFAULT_ENCODING = "UTF-8";

    static final String URI_ID = "uri_id";
    static final String DATA_TYPE = "data_type";
    
    // configurations to be overwritten in hadoop conf
    static final String CONF_INPUT_COMPRESSION_CODEC = 
        "mapreduce.marklogic.input.compressioncodec";
    // for Hadoop1
    static final String CONF_MAX_SPLIT_SIZE1 = "mapred.max.split.size";
    static final String CONF_MIN_SPLIT_SIZE1 = "mapred.min.split.size";
    // for Hadoop2
    static final String CONF_MAX_SPLIT_SIZE2 = 
        "mapreduce.input.fileinputformat.split.maxsize";
    static final String CONF_MIN_SPLIT_SIZE2 = 
        "mapreduce.input.fileinputformat.split.minsize";
    static final String CONF_AGGREGATE_RECORD_ELEMENT = 
        "mapreduce.marklogic.aggregate.recordelement";
    static final String CONF_AGGREGATE_RECORD_NAMESPACE = 
        "mapreduce.marklogic.aggregate.recordnamespace";
    static final String CONF_DELIMITER = 
        "mapreduce.marklogic.delimited.delimiter";
    static final String CONF_DELIMITED_ROOT_NAME = 
        "mapreduce.marklogic.delimited.rootname";
    static final String CONF_SPLIT_INPUT = 
        "mapreduce.marklogic.splitinput";
    static final String CONF_OUTPUT_FILEPATH = 
         "mapreduce.output.fileoutputformat.outputdir";
    static final String CONF_INPUT_FILE_PATTERN = 
        "mapreduce.marklogic.input.filepattern";
    static final String CONF_OUTPUT_FILENAME_AS_COLLECTION = 
        "mapreduce.marklogic.output.filenameascollection";
    static final String CONF_INPUT_SEQUENCEFILE_KEY_CLASS = 
        "mapreduce.marklogic.input.sequencefile.keyclass";
    static final String CONF_INPUT_SEQUENCEFILE_VALUE_CLASS = 
        "mapreduce.marklogic.input.sequencefile.valueclass";
    static final String CONF_INPUT_SEQUENCEFILE_VALUE_TYPE = 
        "mapreduce.marklogic.input.sequencefile.valuetype";
    static final String CONF_OUTPUT_TYPE = "mapreduce.marklogic.output.type";
    static final String CONF_COPY_PERMISSIONS = 
        "mapreduce.marklogic.copypermissions";
    static final String CONF_COPY_PROPERTIES = 
        "mapreduce.marklogic.copyproperties";
    static final String CONF_INPUT_ARCHIVE_METADATA_OPTIONAL = 
        "mapreduce.marklogic.input.archive.metadataoptional";
    static final String CONF_THREADS_PER_SPLIT = 
        "mapreduce.marklogic.multithreadedmapper.threads";
    static final String CONF_MULTITHREADEDMAPPER_CLASS = 
        "mapreduce.marklogic.multithreadedmapper.class";
    static final String CONF_TRANSFORM_MODULE = 
            "mapreduce.marklogic.transformmodule";
    static final String CONF_TRANSFORM_NAMESPACE = 
            "mapreduce.marklogic.transformnamespace";
    static final String CONF_TRANSFORM_FUNCTION = 
            "mapreduce.marklogic.transformfunction";
    static final String CONF_TRANSFORM_PARAM = 
            "mapreduce.marklogic.transformparam";
    static final String CONF_MIMETYPES = 
            "mapreduce.marklogic.mimetypes";
    static final String CONF_MIN_THREADS = 
            "mapreduce.marklogic.minthreads";
    static final String CONF_INPUT_DIRECTORY = 
        "mapreduce.input.fileinputformat.inputdir";
    static final String CONF_INPUT_PATH_FILTER_CLASS = 
        "mapreduce.input.pathFilter.class";
    static final String CONF_MAPREDUCE_JOB_MAP_CLASS = 
        "mapreduce.job.map.class";
    static final String CONF_MAPREDUCE_JOB_WORKING_DIR = 
        "mapreduce.job.working.dir";
    static final String CONF_MAPREDUCE_JOBTRACKER_ADDRESS = 
        "mapreduce.jobtracker.address";
    static final String CONF_INPUT_URI_ID = 
        "mapreduce.marklogic.input.uriid";
    static final String CONF_INPUT_GENERATE_URI = 
        "mapreduce.marklogic.input.generateuri";
    static final String CONF_DELIMITED_DATA_TYPE = 
            "mapreduce.marklogic.delimited.datatype";
    static final String CONF_AUDIT_MLCPSTART_MESSAGE = 
            "mapreduce.marklogic.audit.mlcpstart.message";
    static final String CONF_AUDIT_MLCPFINISH_ENABLED =
            "mapreduce.marklogic.audit.mlcpfinish.enabled";
    static final String CONF_AUDIT_MLCPFINISH_MESSAGE = 
            "mapreduce.marklogic.audit.mlcpfinish.message";
    static final String CONF_INPUT_MODULES_DATABASE =
            "mapreduce.marklogic.input.modules";
    static final String CONF_INPUT_MODULES_ROOT =
            "mapreduce.marklogic.input.modulesroot";

    /**
     * <Role-name,Role-id> map for internal use
     */
    static final String CONF_ROLE_MAP = "mapreduce.marklogic.output.rolemap";
    
    /**
     * MarkLogic Server version
     */
    static final String CONF_ML_VERSION = "mapreduce.marklogic.serverversion";
    static final int MAX_BATCH_SIZE = 200;
    static final int MAX_TXN_SIZE = 4000;
    /**
     * Auditing constants
     */
    static final String AUDIT_MLCPSTART_EVENT = 
            "mlcp-copy-export-start";
    static final String AUDIT_MLCPFINISH_EVENT = 
            "mlcp-copy-export-finish";
    static final String AUDIT_MLCPSTART_CODE =
            "mlcpcopyexportstart";
    static final String AUDIT_MLCPFINISH_CODE = 
            "mlcpcopyexportfinish";
    static final long BATCH_MIN_VERSION = 8000604;

    /**
     * Auto-scaling constants
     */
    static final String POLLING_INIT_DELAY = "polling_init_delay";
    static final String POLLING_PERIOD = "polling_period";
    static final TimeUnit POLLING_TIME_UNIT = TimeUnit.MINUTES;

    /**
     * 11.1.0: Support reverse proxy constants
     */
    static final String BASE_PATH = "base_path";
    static final String INPUT_BASE_PATH = "input_base_path";
    static final String OUTPUT_BASE_PATH = "output_base_path";

    /**
     * 11.1.0: Support token-based authentication constants
     */
    static final String DEFAULT_ML_CLOUD_PORT = "443";
    static final String API_KEY = "api_key";
    static final String INPUT_API_KEY = "input_api_key";
    static final String OUTPUT_API_KEY = "output_api_key";
    static final String TOKEN_ENDPOINT = "token_endpoint";
    static final String INPUT_TOKEN_ENDPOINT = "input_token_endpoint";
    static final String OUTPUT_TOKEN_ENDPOINT = "output_token_endpoint";
    static final String GRANT_TYPE = "grant_type";
    static final String INPUT_GRANT_TYPE = "input_grant_type";
    static final String OUTPUT_GRANT_TYPE = "output_grant_type";
}
