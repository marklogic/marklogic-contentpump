package com.marklogic.mapreduce;

/**
 * Constants used in the package, e.g. configuration property names.
 * 
 * @author jchen
 */

public interface MarkLogicConstants {
    static final String INPUT_USERNAME = 
    	"mapreduce.marklogic.input.username";
    static final String INPUT_PASSWORD = 
    	"mapreduce.marklogic.input.password";
    static final String INPUT_HOST = 
    	"mapreduce.marklogic.input.host";
    static final String INPUT_PORT = 
    	"mapreduce.marklogic.input.port";
    static final String PATH_EXPRESSION = 
    	"mapreduce.marklogic.input.pathexpression";
    static final String PATH_NAMESPACE =
    	"mapreduce.marklogic.input.namespace";
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
    static final String OUTPUT_USERNAME = 
    	"mapreduce.marklogic.output.username";
    static final String OUTPUT_PASSWORD = 
    	"mapreduce.marklogic.output.password";
    static final String OUTPUT_HOST = 
    	"mapreduce.marklogic.output.host";
    static final String OUTPUT_PORT = 
    	"mapreduce.marklogic.output.port";
    static final String OUTPUT_DIRECTORY = 
    	"mapreduce.marklogic.output.directory";
    static final String OUTPUT_COLLECTION =
    	"mapreduce.marklogic.output.collection";
    static final String OUTPUT_PERMISSION = 
    	"mapreduce.marklogic.output.permission";
    static final String OUTPUT_QUALITY = 
    	"mapreduce.marklogic.output.quality";
    static final String OUTPUT_CLEAN_DIR = 
    	"mapreduce.marklogic.output.cleandir";
    static final String NODE_OPERATION_TYPE = 
    	"mapreduce.marklogic.output.node.type";
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
