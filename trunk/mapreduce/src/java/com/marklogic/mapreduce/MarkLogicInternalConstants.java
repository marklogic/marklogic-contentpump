package com.marklogic.mapreduce;

/**
 * Internal constants shared by the package.
 * 
 * @author jchen
 */
public interface MarkLogicInternalConstants {

	// internal constants
	/** Internal Use Only */
	static final String USER_TEMPLATE = "{user}";
	/** Internal Use Only */
	static final String PASSWORD_TEMPLATE = "{password}";
	/** Internal Use Only */
	static final String HOST_TEMPLATE = "{host}";
	/** Internal Use Only */
	static final String PORT_TEMPLATE = "{port}";
	/** Internal Use Only */
	static final String SERVER_URI_TEMPLATE = "xcc://{user}:{password}@{host}:{port}";
	/** Internal Use Only */
	static final String DOCUMENT_SELECTOR_TEMPLATE = "{document_selector}";
	/** Internal Use Only */
	static final String SUBDOCUMENT_EXPR_TEMPLATE = "{subdocument-expr}";
	/** Internal Use Only */
	static final String NAMESPACE_TEMPLATE = "{namespace}";
	/** Internal Use Only */
	static final String DATABASENAME_TEMPLATE = "{database_name}";
	/** Internal Use Only */
	static final String QUERY_TEMPLATE = "{query}";
	/** Internal Use Only */
	static final String NODE_PATH_TEMPLATE = "{node_path}";
	/** Internal Use Only */
	static final String NODE_STRING_TEMPLATE = "{node_string}";

}