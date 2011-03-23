/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

/**
 * Constants used in the package, e.g. configuration property names.
 * Use these property names in your Hadoop configuration to set
 * MarkLogic specific properties. Properties may be set either in
 * a Hadoop configuration file, or programmatically.
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
	 * The config property name (<code>mapreduce.marklogic.com.input.username</code>)
	 * which, if set, specifies the MarkLogic Server user name
	 * under which input queries and operations run. Required if using
	 * MarkLogic Server for input.
	 */
    static final String INPUT_USERNAME = 
    	"mapreduce.marklogic.input.username";
    /**
     * The config property name (<code>mapreduce.marklogic.input.password</code>)
     * which, if set, specifies the cleartext password to use for 
     * authentication with {@link #INPUT_USERNAME input.username}. 
     * Required if using MarkLogic Server for input.
     */
    static final String INPUT_PASSWORD = 
    	"mapreduce.marklogic.input.password";
    /**
     * The config property name (<code>mapreduce.marklogic.input.host</code>)
     * which, if set, specifies the MarkLogic Server host to use for
     * input operations. Required if using MarkLogic Server for input.
     */
    static final String INPUT_HOST = 
    	"mapreduce.marklogic.input.host";
    /**
     * The config property name (<code>mapreduce.marklogic.input.port</code>)
     * which, if set, specifies the port number of the input MarkLogic
     * Server specified by the {@link #INPUT_HOST input.host} property.
     * Required if using MarkLogic Server for input.
     */
    static final String INPUT_PORT = 
    	"mapreduce.marklogic.input.port";
    /**
     * The config property name (<code>mapreduce.marklogic.input.pathexpression</code>)
     * which, if set, specifies the path expression used to retrieve 
     * records from the server. Required if using MarkLogic Server for
     * input in <code>basic</code> mode.
     * 
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
     *   &lt;name&gt;mapreduce.marklogic.input.pathexpression&lt;/name&gt;
     *   &lt;value&gt;fn:collection()//wp:a[@href]&lt;/value&gt;
     * &lt;/property&gt;
     * </pre>
     */
    static final String PATH_EXPRESSION = 
    	"mapreduce.marklogic.input.pathexpression";
    /**
     * The config property name (<code>mapreduce.marklogic.input.namespace</code>)
     * which, if set, specifies a list of namespaces to use when
     * evaluating the path expression in the
     * {@link #PATH_EXPRESSION input.pathexpression} property.
     * Required if using MarkLogic Server for input and running in
     * <code>basic</code> mode; see the 
     * {@link #INPUT_MODE input.mode} property.
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
     * <em>Not yet implemented.</em>
     * <p>
     *  The config property name (<code>mapreduce.marklogic.input.splitquery</code>)
     *  which, if set, specifies the query MarkLogic Server uses 
     *  to generate input splits. Optional.
     * <p>
     *  This property is only usable in <code>advanced</code> mode; 
     *  see the {@link #INPUT_MODE input.mode} property for details.
     * </p>
     * <p>
     *  The default split query uses a rough estimate based on the
     *  number of documents in the database.
     * </p> 
     * <p>
     *  The query must return a sequence containing forest id, record 
     *  count, and a list of host names.
     * </p>
     */
    static final String SPLIT_QUERY = 
    	"mapreduce.marklogic.input.splitquery";
    /**
     *  The config property name (<code>mapreduce.marklogic.input.maxSplitSize</code>)
     *  which, if set, specifies the maximum number of records per 
     *  input split. Optional. Default: 1000. The default should be
     *  suitable for most applications.
     */  
    static final String MAX_SPLIT_SIZE =
    	"mapreduce.marklogic.input.maxSplitSize";
    /**
     * The config property name (<code>mapreduce.marklogic.input.databasename</code>)
     * which, if set, specifies the name of the MarkLogic Server 
     * database from which to create input splits. Required if using
     * MarkLogic Server for input.
     */
    static final String INPUT_DATABASE_NAME = 
    	"mapreduce.marklogic.input.databasename";
    /**
     * The config property name (<code>mapreduce.marklogic.input.recordToFragmentRatio</code>)
     * which, if set, specifies the ratio of the number of retrieved 
     * records to the number of accessed fragments. Optional.
     * Default: 1.0 (one record per fragment).
     * 
     * <p>
     *  The record to fragment ratio can be used in conjunction
     *  with {@link #MAX_SPLIT_SIZE input.maxSplitSize} to
     *  influence the number of map tasks generated. 
     * </p>
     */ 
    static final String RECORD_TO_FRAGMENT_RATIO =
    	"mapreduce.marklogic.input.recordToFragmentRatio";
    /**
     * The config property name (<code>mapreduce.marklogic.input.keyClass</code>)
     * which, if set, specifies the name of the class of the map 
     * input keys. Optional. Default: {@link org.apache.hadoop.io.Text}.
     */
    static final String INPUT_KEY_CLASS = 
    	"mapreduce.marklogic.input.keyClass";
    /**
     * The config property name (<code>mapreduce.marklogic.input.valueClass</code>)
     * which, if set, specifies the name of the class of the map 
     * input value. Optional. Default: {@link org.apache.hadoop.io.Text}.
     */
    static final String INPUT_VALUE_CLASS = 
    	"mapreduce.marklogic.input.valueClass";
    /**
     * The config property name (<code>mapreduce.marklogic.input.mode</code>)
     * which, if set, specifies the whether to use basic or advanced
     * input query mode. Allowable values are <code>basic</code> and
     * <code>advanced</code>. Optional. Default: <code>basic</code>.
     * 
     * <p>
     *  Basic mode enables use of the {@link #PATH_EXPRESSION input.pathexpression}
     *  and {@link #PATH_NAMESPACE input.namespace} properties. Advanced
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
     * The config property name (<code>mapreduce.marklogic.input.query</code>)
     * which, if set, specifies the query used to retrieve input
     * records from MarkLogic Server. This property is only usable
     * when <code>advanced</code> is specified in the
     * {@link #INPUT_MODE input.mode} property.
     * 
     * <p>
     *  The value of this property must be a fully formed query,
     *  suitable for evaluation by <code>xdmp:eval</code>, and
     *  must return a sequence of key-value pairs consistent with
     *  the classes used in {@link #INPUT_KEY_CLASS input.keyClass}
     *  and {@link #INPUT_VALUE_CLASS input.valueClass}.
     * </p>
     */
    static final String INPUT_QUERY =
    	"mapreduce.marklogic.input.query";
    
    // output-related config property names
	/**
	 * The config property name (<code>mapreduce.marklogic.com.output.username</code>)
	 * which, if set, specifies the MarkLogic Server user name
	 * under which output operations run. Required if using MarkLogic
	 * Server for output.
	 */
    static final String OUTPUT_USERNAME = 
    	"mapreduce.marklogic.output.username";
    /**
     * The config property name (<code>mapreduce.marklogic.output.password</code>)
     * which, if set, specifies the cleartext password to use for 
     * authentication with {@link #OUTPUT_USERNAME output.username}. 
     * Required if using MarkLogic Server for output.
     */
    static final String OUTPUT_PASSWORD = 
    	"mapreduce.marklogic.output.password";
    /**
     * The config property name (<code>mapreduce.marklogic.output.host</code>)
     * which, if set, specifies the MarkLogic Server host to use for
     * output operations. Required if using MarkLogic Server for
     * output.
     */
    static final String OUTPUT_HOST = 
    	"mapreduce.marklogic.output.host";
    /**
     * The config property name (<code>mapreduce.marklogic.output.port</code>)
     * which, if set, specifies the port number of the output MarkLogic
     * Server specified by the {@link #INPUT_HOST input.host} property.
     * Required if using MarkLogic Server for output.
     */
    static final String OUTPUT_PORT = 
    	"mapreduce.marklogic.output.port";
    /**
     * The config property name (<code>mapreduce.marklogic.output.directory</code>)
     * which, if set, specifies the MarkLogic Server database directory
     * where output documents are created. Required if using MarkLogic
     * Server for output with document output format.
     */
    static final String OUTPUT_DIRECTORY = 
    	"mapreduce.marklogic.output.directory";
    /**
     * The config property name (<code>mapreduce.marklogic.output.collection</code>)
     * which, if set, specifies a comma-separated list of collections
     * to which generated output documents are added. Optional. Relevant
     * only when using MarkLogic Server for output with document output
     * format.
     * 
     * <p>Example:</p>
     * 
     * <pre class="codesample">
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.output.collection&lt;/name&gt;
     *   &lt;value&gt;latest,top10&lt;/value&gt;
     * &lt;/property&gt;
     * </pre>
     */
    static final String OUTPUT_COLLECTION =
    	"mapreduce.marklogic.output.collection";
    /**
     * The config property name (<code>mapreduce.marklogic.output.permission</code>)
     * which, if set, specifies a comma-separated list role-capability
     * pairs to associate with created output documents. Optional. If
     * not set, the default permissions for 
     * {@link #OUTPUT_USERNAME output.username} are used. Relevant
     * only when using MarkLogic Server for output with document
     * output format.
     * 
     * <p>Example:</p>
     * 
     * <pre class="codesample">
     * &lt;property&gt;
     *   &lt;name&gt;mapreduce.marklogic.output.permission&lt;/name&gt;
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
     *  role name, you must set this property programmatically,
     *  rather than in a configuration file.
     *  </p>
     */
    static final String OUTPUT_PERMISSION = 
    	"mapreduce.marklogic.output.permission";
    /**
     * The config property name (<code>mapreduce.marklogic.output.quality</code>)
     * which, if set, specifies the document quality for created
     * output documents. Optional. Relevant only when using MarkLogic
     * Server for output with document output format.
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
    	"mapreduce.marklogic.output.quality";
    /**
     * The config property name (<code>mapreduce.marklogic.output.cleandir</code>)
     * which, if set, indicates whether or not to remove the output
     * directory. Only applicable to document output. Default: true.
     * 
     * <p>
     *  When set to true, the output directory specified by the
     *  {@link #OUTPUT_DIRECTORY output.directory} property is
     *  removed. When set to false, an exception is thrown if
     *  the output directory already exists.
     * </p>
     */
    static final String OUTPUT_CLEAN_DIR = 
    	"mapreduce.marklogic.output.cleandir";
    /**
     * The config property name (<code>mapreduce.marklogic.output.nodeOpType</code>)
     * which, if set, indicates what node operation to perform
     * during output. Required if using MarkLogic Server for output
     * with node output format.
     * 
     * @see NodeOpType
     * @see NodeOutputFormat
     * @see NodeWriter
     */
    static final String NODE_OPERATION_TYPE = 
    	"mapreduce.marklogic.output.nodeOpType";
    /**
     * The config property name (<code>mapreduce.marklogic.output.nodeOpType</code>)
     * which, if set, indicates the namespace used for output.
     */
    static final String OUTPUT_NAMESPACE = 
    	"mapreduce.marklogic.output.namespace";
    
    // internal constants
    static final String USER_TEMPLATE = "{user}";
    static final String PASSWORD_TEMPLATE = "{password}";
    static final String HOST_TEMPLATE = "{host}";
    static final String PORT_TEMPLATE = "{port}";
    static final long DEFAULT_MAX_SPLIT_SIZE = 1000;
	static final String SERVER_URI_TEMPLATE =
    	"xcc://{user}:{password}@{host}:{port}";
	static final String PATH_EXPRESSION_TEMPLATE = "{path_expression}";
	static final String NAMESPACE_TEMPLATE = "{namespace}";
	static final String DATABASENAME_TEMPLATE = "{database_name}";
	static final String QUERY_TEMPLATE = "{query}";
	static final String NODE_PATH_TEMPLATE = "{node_path}";
    static final String NODE_STRING_TEMPLATE = "{node_string}";
}
