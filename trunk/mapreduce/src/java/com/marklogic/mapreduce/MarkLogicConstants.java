package com.marklogic.mapreduce;

/**
 * Constants used in the package, e.g. configuration property names.
 * 
 * @author jchen
 */

public interface MarkLogicConstants {
	// input-related config property names
    static final String INPUT_USERNAME = 
    	"mapreduce.marklogic.input.username";
    static final String INPUT_PASSWORD = 
    	"mapreduce.marklogic.input.password";
    static final String INPUT_HOST = 
    	"mapreduce.marklogic.input.host";
    static final String INPUT_PORT = 
    	"mapreduce.marklogic.input.port";
    /**
     * Path expression used to retrieve records from the server.
     */
    static final String PATH_EXPRESSION = 
    	"mapreduce.marklogic.input.pathexpression";
    static final String PATH_NAMESPACE =
    	"mapreduce.marklogic.input.namespace";
    /**
     * Query to be issued to server that returns a sequence of forest id, 
     * record count and host names.
     */
    static final String SPLIT_QUERY = 
    	"mapreduce.marklogic.input.splitquery";
    /**
     *  Maximum number of records per each split. 
     */  
    static final String MAX_SPLIT_SIZE =
    	"mapreduce.marklogic.input.maxSplitSize";
    static final String INPUT_DATABASE_NAME = 
    	"mapreduce.marklogic.input.databasename";
    /**
     *  Ratio of number of retrieved records to number of accessed fragments.
     */ 
    static final String RECORD_TO_FRAGMENT_RATIO =
    	"mapreduce.marklogic.input.recordToFragmentRatio";
    /**
     * Class to be used for storing input keys.
     */
    static final String INPUT_KEY_CLASS = 
    	"mapreduce.marklogic.input.keyClass";
    /**
     * Class to be used for storing input values.
     */
    static final String INPUT_VALUE_CLASS = 
    	"mapreduce.marklogic.input.valueClass";
    // output-related config property names
    static final String OUTPUT_USERNAME = 
    	"mapreduce.marklogic.output.username";
    static final String OUTPUT_PASSWORD = 
    	"mapreduce.marklogic.output.password";
    static final String OUTPUT_HOST = 
    	"mapreduce.marklogic.output.host";
    static final String OUTPUT_PORT = 
    	"mapreduce.marklogic.output.port";
    /**
     * Directory used for document output.
     */
    static final String OUTPUT_DIRECTORY = 
    	"mapreduce.marklogic.output.directory";
    /**
     * Collection(s) used for document output.
     */
    static final String OUTPUT_COLLECTION =
    	"mapreduce.marklogic.output.collection";
    /**
     * Permission(s) used for document output.
     */
    static final String OUTPUT_PERMISSION = 
    	"mapreduce.marklogic.output.permission";
    /**
     * Quality used for document output.
     */
    static final String OUTPUT_QUALITY = 
    	"mapreduce.marklogic.output.quality";
    /**
     * Remove target directory if set to true; otherwise, throw exception if
     * the target directory already exists.  Only applicable to document 
     * output.
     */
    static final String OUTPUT_CLEAN_DIR = 
    	"mapreduce.marklogic.output.cleandir";
    /**
     * Type of operation for node output.  See {@link NodeOpType}.
     */
    static final String NODE_OPERATION_TYPE = 
    	"mapreduce.marklogic.output.nodetype";
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
}
