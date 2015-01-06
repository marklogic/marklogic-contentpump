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
package com.marklogic.mapreduce;


/**
 * Configuration property names and other constants used in the
 * package. Use these property names in your Hadoop configuration
 * to set MarkLogic specific properties. Properties may be set
 * either in a Hadoop configuration file or programatically.
 * 
 * <p>
 *  Use the <code>mapreduce.marklogic.input.*</code> properties when
 *  using MarkLogic Server as an input source. Use the
 *  <code>mapreduce.marklogic.output.*</code> properties when using
 *  MarkLogic Server to store your results.
 * </p>
 * 
 * @author jchen
 */

public interface MarkLogicConstants {
    // input-related config property names
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the MarkLogic Server user name
     * under which input queries and operations run. Required if using
     * MarkLogic Server for input.
     */
    static final String INPUT_USERNAME = 
        "mapreduce.marklogic.input.username";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the cleartext password to use for 
     * authentication with {@link #INPUT_USERNAME input.username}. 
     * Required if using MarkLogic Server for input.
     */
    static final String INPUT_PASSWORD = 
        "mapreduce.marklogic.input.password";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the MarkLogic Server host to use for
     * input operations. Required if using MarkLogic Server for input.
     */
    static final String INPUT_HOST = 
        "mapreduce.marklogic.input.host";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the port number of the input XDBC 
     * server on the MarkLogic Server host specified by the 
     * {@link #INPUT_HOST input.host} property. Required if using 
     * MarkLogic Server for input.
     * 
     * <p>
     *  <strong>NOTE:</strong> Within a cluster, all nodes supplying 
     *  MapReduce input data must use the same XDBC server port number.
     * </p>
     */
    static final String INPUT_PORT = 
        "mapreduce.marklogic.input.port";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies whether the connection to the input server is 
     * SSL enabled; false is assumed if not set.
     */
    static final String INPUT_USE_SSL = "mapreduce.marklogic.input.usessl";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the name of the class implementing 
     * {@link SslConfigOptions} which will be used if 
     * {@link #INPUT_USE_SSL input.ssl} is set to true.
     */
    static final String INPUT_SSL_OPTIONS_CLASS = 
        "mapreduce.marklogic.input.ssloptionsclass"; 
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the document selection portion of the
     * path expression used to retrieve data from the server. Only
     * used if using MarkLogic Server for input in <code>basic</code> mode.
     * 
     * <p>
     *  The XQuery path expression step given in this property must
     *  select a sequence of document nodes. To further refine the
     *  input selection to nodes or values within the documents, use 
     *  {@link #SUBDOCUMENT_EXPRESSION input.subdocumentexpr}. If 
     *  this property is not set, <code>fn:collection()</code> is used.
     *  For more information, see the overview.
     * </p>
     * 
     * <p>
     *  This property is only usable when <code>basic</code> mode is 
     *  specified with the {@link #INPUT_MODE input.mode} property. If
     *  more powerful input customization is needed, use 
     *  <code>advanced</code> mode and specify a complete input query 
     *  with the {@link #INPUT_QUERY input.query} property.
     * </p>
     * 
     * <p>
     *  The path expression step given in this property must be 
     *  <em>searchable</em>. A searchable expression is one which can
     *  be optimized using indexes. See the <em>Query and Performance
     *  Tuning Guide</em> for more information on searchable path
     *  expressions.
     * </p>
     * 
     * <p>The following selects all documents:</p>
     * 
     * <pre class="codesample">
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.input.documentselector&lt;/name&gt;
     *   &lt;value&gt;fn:collection()&lt;/value&gt;
     * &lt;/property&gt;
     * </pre>
     */
    static final String DOCUMENT_SELECTOR = 
        "mapreduce.marklogic.input.documentselector";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the path expression used to retrieve 
     * sub-document records from the server. Used only if using MarkLogic 
     * Server for input in <code>basic</code> mode. If not set,
     * the document nodes selected by the {@link #DOCUMENT_SELECTOR
     * document selector} are used.
     * 
     * <p>
     *  The XQuery path expression step given in this property should
     *  select a sequence of nodes or atomic values from the set of
     *  documents selected by the path step given in the
     *  {@link #DOCUMENT_SELECTOR input.documentselector} property. 
     *  For more information, see the overview.
     * </p>
     * <p>
     *  This property is only usable when <code>basic</code> mode is 
     *  specified with the {@link #INPUT_MODE input.mode} property. If
     *  more powerful input customization is needed, use 
     *  <code>advanced</code> mode and specify a complete input query 
     *  with the {@link #INPUT_QUERY input.query} property.
     * </p>
     * 
     * <p>The following would select all documents containing hrefs:</p>
     * 
     * <pre class="codesample">
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.input.documentselector&lt;/name&gt;
     *   &lt;value&gt;fn:collection()&lt;/value&gt;
     * &lt;/property&gt;
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.input.subdocumentexpr&lt;/name&gt;
     *   &lt;value&gt;//wp:a[@href]&lt;/value&gt;
     * &lt;/property&gt;
     * </pre>
     */
    static final String SUBDOCUMENT_EXPRESSION = 
        "mapreduce.marklogic.input.subdocumentexpr";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the name of the class implementing 
     * {@link com.marklogic.mapreduce.functions.LexiconFunction LexiconFunction} 
     * which will be used to generate input. 
     */
    static final String INPUT_LEXICON_FUNCTION_CLASS =
        "mapreduce.marklogic.input.lexiconfunctionclass";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies a list of namespaces to use when
     * evaluating the path expression constructed from the
     * {@link #DOCUMENT_SELECTOR input.documentselector} and
     * {@link #SUBDOCUMENT_EXPRESSION input.subdocumentexpr} properties.
     * 
     * <p>Specify the namespaces as comma separated alias-URI pairs.
     *  For example:
     * </p>
     * 
     * <pre class="codesample">
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.input.namespace&lt;/name&gt;
     *   &lt;value&gt;wp, "http://www.mediawiki.org.xml/export-0.4/"&lt;/value&gt;
     * &lt;/property&gt;
     * </pre>
     * 
     * <p>
     *  If a namespace URI includes a comma, you must set this
     *  property programmatically, rather than in a config file.
     * </p>
     */
    static final String PATH_NAMESPACE =
        "mapreduce.marklogic.input.namespace";
    /**
     *  The config property name (<code>{@value}</code>)
     *  which, if set, specifies the query MarkLogic Server uses 
     *  to generate input splits. This property is required (and only 
     *  usable) in <code>advanced</code> mode; see the
     *  {@link #INPUT_MODE input.mode} property for details.
     * </p>
     * <p>
     *  The split query must return a sequence of (forest id, record 
     *  count, hostname) tuples. The host name and forest id identify
     *  the forest associated with the split. The count is an estimate
     *  of the number of key-value pairs in the split.
     * </p>
     * <p>
     *  The default split query used in <code>basic</code> input mode
     *  computes a rough estimate based on the number of documents in 
     *  the database.
     * </p> 
     */
    static final String SPLIT_QUERY = 
        "mapreduce.marklogic.input.splitquery";
    /**
     *  The config property name (<code>{@value}</code>)
     *  which, if set, specifies the maximum number of fragments per 
     *  input split. Optional. Default: {@value #DEFAULT_MAX_SPLIT_SIZE}. 
     *  The default should be suitable for most applications.
     */  
    static final String MAX_SPLIT_SIZE =
        "mapreduce.marklogic.input.maxsplitsize";
    /**
     * Not yet Implemented.
     * 
     * <p>
     *   The config property name (<code>{@value}</code>)
     *   which, if set, specifies the name of the MarkLogic Server 
     *   database from which to create input splits. 
     * </p>
     */
    static final String INPUT_DATABASE_NAME = 
        "mapreduce.marklogic.input.databasename";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the name of the class of the map 
     * input keys for {@link KeyValueInputFormat}. Optional. 
     * Default: {@link org.apache.hadoop.io.Text}.
     */
    static final String INPUT_KEY_CLASS = 
        "mapreduce.marklogic.input.keyclass";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the name of the class of the map 
     * input value for {@link KeyValueInputFormat}, {@link ValueInputFormat} 
     * and {@link DocumentInputFormat}. 
     * Optional. Default: {@link org.apache.hadoop.io.Text} for 
     * {@link KeyValueInputFormat} and {@link ValueInputFormat}, 
     * {@link DatabaseDocument} for {@link DocumentInputFormat}.
     */
    static final String INPUT_VALUE_CLASS = 
        "mapreduce.marklogic.input.valueclass";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies whether to use basic or advanced
     * input query mode. Allowable values are <code>basic</code> and
     * <code>advanced</code>. Optional. Default: <code>basic</code>.
     * 
     * <p><em>Only basic mode is supported at this time.</em></p>
     * 
     * <p>
     *  Basic mode enables use of the 
     *  {@link #DOCUMENT_SELECTOR input.documentselector},
     *  {@link #SUBDOCUMENT_EXPRESSION input.subdocumentexpr}, and
     *  {@link #PATH_NAMESPACE input.namespace} properties. Advanced
     *  mode enables use of the {@link #INPUT_QUERY input.query} and
     *  {@link #SPLIT_QUERY input.splitquery} properties.
     * </p>
     */
    static final String INPUT_MODE = 
        "mapreduce.marklogic.input.mode";
    /**
     * Value string of basic mode for {@link #INPUT_MODE input.mode}.
     */
    static final String BASIC_MODE = "basic";
    /**
     * Value string of advanced mode for {@link #INPUT_MODE input.mode}.
     */
    static final String ADVANCED_MODE = "advanced";
    /**
     *  The config property name (<code>{@value}</code>)
     *  which, if set, specifies the query used to retrieve input
     *  records from MarkLogic Server. This property is required
     *  when <code>advanced</code> is specified in the
     *  {@link #INPUT_MODE input.mode} property.
     * </p>
     * 
     * <p>
     *  The value of this property must be a fully formed query,
     *  suitable for evaluation by <code>xdmp:eval</code>, and
     *  must return a sequence. The items in the sequence depend
     *  on the {@link org.apache.hadoop.mapreduce.InputFormat InputFormat}
     *  subclass configured for the job. For details, see 
     *  "Advanced Input Mode" in the <em>Hadoop MapReduce Connector
     *  Developer's Guide</em>.
     * </p>
     */
    static final String INPUT_QUERY =
        "mapreduce.marklogic.input.query";
    /**
     *  The config property name (<code>{@value}</code>)
     *  which, if set, specifies data retrieval from MarkLogic Server at the 
     *  specified timestamp. 
     * </p>
     */
    static final String INPUT_QUERY_TIMESTAMP = 
        "mapreduce.marklogic.input.querytimestamp";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set to true, specifies that the input query declares and 
     * references external variables <code>{@value #SPLIT_START_VARNAME}</code>
     * and <code>{@value #SPLIT_END_VARNAME}</code> under the 
     * namespace {@value #MR_NAMESPACE}.  The connector binds to
     * these variables with the start and end of an input split
     * instead of constraining the query with the split range.
     * 
     * <p>
     *  For details, see "Optimizing Your Input Query" in the <em>Hadoop 
     *  MapReduce Connector Developer's Guide</em>.
     * </p>
     */
    static final String BIND_SPLIT_RANGE = 
        "mapreduce.marklogic.input.bindsplitrange";
    /**
     * The namespace ({@value}) in which the split range external variables 
     * are defined.
     * 
     * <p>
     *  The split range variables <code>{@value #SPLIT_START_VARNAME}</code> 
     *  and <code>{@value #SPLIT_END_VARNAME}</code> are in this namespace when
     *  using advanced input mode and <code>{@value #BIND_SPLIT_RANGE}</code>
     *  is true. Declare a namespace prefix for this namespace in your input 
     *  query and qualify references to <code>{@value #SPLIT_START_VARNAME}</code>
     *  and <code>{@value #SPLIT_END_VARNAME}</code> by the prefix. For details,
     *  see "Optimizing Your Input Query" in the <em>Hadoop MapReduce Connector
     *  Developer's Guide</em>.
     * </p>
     */
    static final String MR_NAMESPACE = "http://marklogic.com/hadoop";
    /**
     * Use this external variable name (<code>{@value}</code>) in your advanced
     * mode input query to access the start value of the record range in an
     * input split when <code>{@value #BIND_SPLIT_RANGE}</code> is true. 
     * 
     * <p>
     * The variable must be declared and referenced in the namespace
     * <code>{@value #MR_NAMESPACE}</code>. For details, see
     *  "Optimizing Your Input Query" in the <em>Hadoop MapReduce Connector
     *  Developer's Guide</em>.
     * </p>
     */
    static final String SPLIT_START_VARNAME = "splitstart";
    /**
     * Use this external variable name (<code>{@value}</code>) in your advanced
     * mode input query to access the end value of the record range in an input
     * split when <code>{@value #BIND_SPLIT_RANGE}</code> is true. 
     * 
     * <p>
     * The variable must be declared and referenced in the namespace
     * <code>{@value #MR_NAMESPACE}</code>. For details, see
     *  "Optimizing Your Input Query" in the <em>Hadoop MapReduce Connector
     *  Developer's Guide</em>.
     *  </p>
     */
    static final String SPLIT_END_VARNAME = "splitend";
    /**      
     * The config property name (<code>{@value}</code>) which, if   
     * set, specifies the ratio of the number of retrieved       
     * records to the number of accessed fragments. Optional.      
     * Default: 1.0 (one record per fragment) for documents, 
     * 100 for nodes and values.      
     *       
     * <p>      
     *  The record to fragment ratio is used for progress estimate.       
     * </p>      
     */       
     static final String RECORD_TO_FRAGMENT_RATIO =      
         "mapreduce.marklogic.input.recordtofragmentratio"; 
     /**      
      * The config property name (<code>{@value}</code>) which, if   
      * set, specifies whether to format data with indentation retrieved from 
      * MarkLogic.  Optional. Valid values: TRUE, FALSE, SERVERDEFAULT.
      * Default: false.     
      */       
      static final String INDENTED =      
          "mapreduce.marklogic.input.indented"; 
      /**
       * The config property name (<code>{@value}</code>)
       * which, if set, indicates to only include documents with one or many of
       * specified collection URIs when using {@link ForestInputFormat}.
       */
      static final String COLLECTION_FILTER =
          "mapreduce.marklogic.input.filter.collection";
      /**
       * The config property name (<code>{@value}</code>)
       * which, if set, indicates to only include documents with one of 
       * specified directory URIs when using {@link ForestInputFormat}.
       */
      static final String DIRECTORY_FILTER =
          "mapreduce.marklogic.input.filter.directory";
      /**
       * The config property name (<code>{@value}</code>)
       * which, if set, indicates to only include documents with one of 
       * specified types when using {@link ForestInputFormat}.
       */
      static final String TYPE_FILTER =
          "mapreduce.marklogic.input.filter.type";
    
    // output-related config property names
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the MarkLogic Server user name
     * under which output operations run. Required if using MarkLogic
     * Server for output.
     */
    static final String OUTPUT_USERNAME = 
        "mapreduce.marklogic.output.username";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the cleartext password to use for 
     * authentication with {@link #OUTPUT_USERNAME output.username}. 
     * Required if using MarkLogic Server for output.
     */
    static final String OUTPUT_PASSWORD = 
        "mapreduce.marklogic.output.password";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the MarkLogic Server host to use for
     * output operations. Required if using MarkLogic Server for
     * output.
     */
    static final String OUTPUT_HOST = 
        "mapreduce.marklogic.output.host";
    
    /** Internal use only. */
    static final String OUTPUT_FOREST_HOST = 
        "mapreduce.marklogic.output.hostforests";   
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the port number of the output MarkLogic
     * Server specified by the {@link #INPUT_HOST input.host} property.
     * Required if using MarkLogic Server for output.
     */
    static final String OUTPUT_PORT = 
        "mapreduce.marklogic.output.port";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the MarkLogic Server database to use for
     * output operations. The default value is the target database assigned
     * to the AppServer.
     * .
     */
    static final String OUTPUT_DATABASE_NAME = 
        "mapreduce.marklogic.output.databasename";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies whether the connection to the output server is
     * SSL enabled; false is assumed if not set.
     */
    static final String OUTPUT_USE_SSL = "mapreduce.marklogic.output.usessl";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the name of the class implementing 
     * {@link SslConfigOptions} which will be used if 
     * {@link #OUTPUT_USE_SSL output.usessl} is set to true.
     */
    static final String OUTPUT_SSL_OPTIONS_CLASS = 
        "mapreduce.marklogic.output.ssloptionsclass"; 
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the MarkLogic Server database directory
     * where output documents are created. 
     * <p>
     *  If {@link #OUTPUT_CLEAN_DIR output.cleandir} is false (the default)
     *  then an error occurs if the directory already exists. If {@link
     *  #OUTPUT_CLEAN_DIR output.cleandir} is true, then the directory
     *  is removed as part of the job submission process.
     * </p>
     */
    static final String OUTPUT_DIRECTORY = 
        "mapreduce.marklogic.output.content.directory";
    /**
     * The config property name (<code>{@value}</code>) which, if set,
     * specifies the charset encoding to be used by the server when loading
     * this document. The encoding provided will be passed to the server at
     * document load time and must be a name that it recognizes. The document
     * byte stream will be transcoded to UTF-8 for storage.
     */
    static final String OUTPUT_CONTENT_ENCODING = 
        "mapreduce.marklogic.output.content.encoding";
    /**
     * Default output content encoding
     */
    static final String DEFAULT_OUTPUT_CONTENT_ENCODING = "UTF-8";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies a comma-separated list of collections
     * to which generated output documents are added. Optional. Relevant
     * only when using MarkLogic Server for output with
     * {@link ContentOutputFormat}.
     * 
     * <p>Example:</p>
     * 
     * <pre class="codesample">
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.output.content.collection&lt;/name&gt;
     *   &lt;value&gt;latest,top10&lt;/value&gt;
     * &lt;/property&gt;
     * </pre>
     */
    static final String OUTPUT_COLLECTION =
        "mapreduce.marklogic.output.content.collection";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies a comma-separated list role-capability
     * pairs to associate with created output documents. Optional. If
     * not set, the default permissions for 
     * {@link #OUTPUT_USERNAME output.username} are used. Relevant
     * only when using MarkLogic Server for output with 
     * {@link ContentOutputFormat}.
     * 
     * <p>Example:</p>
     * 
     * <pre class="codesample">
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.output.content.permission&lt;/name&gt;
     *   &lt;value&gt;dls-user,update,dls-user,read&lt;/value&gt;
     * &lt;/property&gt;
     * </pre>
     * 
     * <p>
     *  See "URI Privileges and Permissions on Documents" in the
     *  <em>Understanding and Using Security Guide</em> for more
     *  information about roles and capabilities.
     * </p>
     * 
     * <p>
     *  If the property value includes a comma in embedded in the
     *  role name, you must set this property in your code,
     *  rather than in a configuration file.
     *  </p>
     */
    static final String OUTPUT_PERMISSION = 
        "mapreduce.marklogic.output.content.permission";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the document quality for created
     * output documents. Optional. Relevant only when using MarkLogic
     * Server for output with {@link ContentOutputFormat}.
     * 
     * <p>
     *  Quality affects the search relevance of a document. The
     *  value must be a positive or negative integer. For more
     *  information about document quality, see "Relevance Scores:
     *  Understanding and Customizing" in the <em>Search Developer's
     *  Guide</em>.
     * </p>
     */
    static final String OUTPUT_QUALITY = 
        "mapreduce.marklogic.output.content.quality";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies whether to use streaming to insert
     * content.  When streaming is set to true, the content will 
     * not be fully buffered in memory, hence will consume less
     * memory but will disable auto-retry if there is a problem 
     * inserting the content.
     */
    static final String OUTPUT_STREAMING =
        "mapreduce.marklogic.output.content.streaming";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates whether or not to remove the output
     * directory. Only applicable to {@link ContentOutputFormat}. 
     * Default: false.
     * 
     * <p>
     *  When set to true, the output directory specified by the
     *  {@link #OUTPUT_DIRECTORY output.content.directory} property
     *  is removed. When set to false, an exception is thrown if
     *  the output content directory already exists.
     * </p>
     */
    static final String OUTPUT_CLEAN_DIR = 
        "mapreduce.marklogic.output.content.cleandir";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates whether or not to use the fast load mode
     * to load content into MarkLogic.  Default: false.
     * 
     * <p>
     * Setting it to true when the documents
     * to be loaded already exist may cause XDMP-DBDUPURI error if the
     * original documents were inserted when the database had a different
     * forest count.  The fast load mode will always be
     * used if "mapreduce.marklogic.output.content.directory" is set.
     * </p>
     */
    static final String OUTPUT_FAST_LOAD =
        "mapreduce.marklogic.output.content.fastload";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates what node operation to perform
     * during output. Required if using MarkLogic Server for output
     * with NodeOutputFormat.  Valid choices: INSERT_BEFORE, INSERT_AFTER,
     * INSERT_CHILD, REPLACE.
     * 
     * @see NodeOpType
     * @see NodeOutputFormat
     */
    static final String NODE_OPERATION_TYPE = 
        "mapreduce.marklogic.output.node.optype";
    
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set to true, causes {@link PropertyOutputFormat}
     * to create document properties for reduce output
     * key-value pairs even when no document exists with 
     * the target URI. Default: false.
     * 
     * <p>
     *  By default, {@link PropertyOutputFormat} does not create a
     *  property for a document URI unless the document already 
     *  exists.
     * </p>
     */
    static final String OUTPUT_PROPERTY_ALWAYS_CREATE =
        "mapreduce.marklogic.output.property.alwayscreate";
    
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates the namespace used for output.
     * This is used only in NodeOutputFormat, and is used for 
     * resolving element names in the node path.
     */
    static final String OUTPUT_NAMESPACE = 
        "mapreduce.marklogic.output.node.namespace";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates whether the job is running in local or 
     * distributed mode.
     */
    static final String EXECUTION_MODE = "mapreduce.marklogic.mode";
    static final String MODE_DISTRIBUTED = "distributed";
    static final String MODE_LOCAL = "local";
    /** 
     * The default maximum split size for input splits, used if
     * {@link #MAX_SPLIT_SIZE input.maxsplitsize} is not specified.
     */
    static final long DEFAULT_MAX_SPLIT_SIZE = 50000;
    
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates what property operation to perform
     * during output when using {@link PropertyOutputFormat}. Ignored  
     * if not using {@link PropertyOutputFormat}. Optional. Valid choices: 
     * SET_PROPERTY, ADD_PROPERTY.  Default: SET_PROPERTY.
     * 
     * @see PropertyOpType
     * @see PropertyOutputFormat
     * @see PropertyWriter
     */
    static final String PROPERTY_OPERATION_TYPE = 
        "mapreduce.marklogic.output.property.optype";
    /**
     * Default property operation type.
     */
    static final String DEFAULT_PROPERTY_OPERATION_TYPE = "SET_PROPERTY";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates type of content to be inserted when using 
     * ContentOutputFormat.  Optional.  Valid choices: XML, JSON, TEXT, BINARY,
     * MIXED, UNKNOWN.
     * Default: XML.
     */
    static final String CONTENT_TYPE = 
        "mapreduce.marklogic.output.content.type";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the data type of the  
     * output keys for {@link KeyValueOutputFormat}. Optional. 
     * Default: xs:string.
     */
    static final String OUTPUT_KEY_TYPE = 
        "mapreduce.marklogic.output.keytype";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the data type of the map 
     * output value for {@link KeyValueOutputFormat}. 
     * Optional. Default: xs:string.
     */
    static final String OUTPUT_VALUE_TYPE = 
        "mapreduce.marklogic.output.valuetype";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the statement to execute against MarkLogic 
     * Server. This property is required for KeyValueOutputFormat.
     * 
     * <p>
     * The statement is allowed to declare and refernce two external variables 
     * "key" and "value" under namespace "http://marklogic.com/hadoop", which 
     * will be bound by the connector with the output key and value in the user
     * specified data type.
     * </p>
     */
    static final String OUTPUT_QUERY =
        "mapreduce.marklogic.output.query";
    /**
     * Value string of the output key external variable name.
     */
    static final String OUTPUT_KEY_VARNAME = "key";
    /**
     * The config property name (<code>{@value}</code>) which, if set,
     * specifies the language name to associate with inserted documents. A
     * value of <code>en</code> indicates that the document is in english. The
     * default is null, which indicates to use the server default.
     */
    static final String OUTPUT_CONTENT_LANGUAGE = 
        "mapreduce.marklogic.output.content.language";
    /**
     * The config property name (<code>{@value}</code>) which, if set,
     * specifies the namespace to associate with inserted documents. The 
     * default is null, which indicates that the default namespace should 
     * be used.
     */
    static final String OUTPUT_CONTENT_NAMESPACE = 
        "mapreduce.marklogic.output.content.namespace";
    /**
     * Value string of the output value external variable name.
     */
    static final String OUTPUT_VALUE_VARNAME = "value";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the document repair level for this options object. 
     */
    static final String OUTPUT_XML_REPAIR_LEVEL = 
        "mapreduce.marklogic.output.content.repairlevel";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies whether to tolerate insertion errors and 
     * make sure all successful inserts are committed. 
     */
    static final String OUTPUT_TOLERATE_ERRORS = 
        "mapreduce.marklogic.output.content.tolerateerrors";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, specifies the partition  
     * where output documents are created. 
     */
    static final String OUTPUT_PARTITION =
        "mapreduce.marklogic.output.partition";
    
    /**
     * Default output XML repair level
     */
    static final String DEFAULT_OUTPUT_XML_REPAIR_LEVEL = "DEFAULT";
    /**
     * Default content type.
     */
    static final String DEFAULT_CONTENT_TYPE = "XML";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates the number of records in one request.  
     * Optional.  Currently only applies to ContentOutputFormat.
     */
    static final String BATCH_SIZE = 
        "mapreduce.marklogic.output.batchsize";
    /**
     * Default batch size.
     */
    static final int DEFAULT_BATCH_SIZE = 100;
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates the number of requests in one transaction. 
     * Optional.
     */
    static final String TXN_SIZE = 
        "mapreduce.marklogic.output.transactionsize";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates assignment policy for output documents. 
     * Optional.
     */    
    static final String ASSIGNMENT_POLICY =
        "mapreduce.marklogic.output.assignmentpolicy";
    /**
     * The config property name (<code>{@value}</code>)
     * which, if set, indicates temporal collection for documents. 
     * Optional.
     */    
    static final String TEMPORAL_COLLECTION =
        "mapreduce.marklogic.output.temporalcollection";
    /**
     * <Role-name,Role-id> map for internal use
     */
    static final String ROLE_MAP = "mapreduce.marklogic.output.rolemap";
}
