/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.ItemType;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XSString;

/**
 * MarkLogic-based InputFormat, taking a generic key class.
 * 
 * @author jchen
 */
public abstract class MarkLogicInputFormat<KEYIN, VALUEIN> extends InputFormat<KEYIN, VALUEIN> 
implements MarkLogicConstants {
	public static final Log LOG =
	    LogFactory.getLog(MarkLogicInputFormat.class);
	/* save later after is-searchable is available
	static final String SPLIT_QUERY_TEMPLATE = 
    	"xquery version \"1.0-ml\"; \n" + 
    	NAMESPACE_TEMPLATE + 
        "import module namespace admin = \"http://marklogic.com/xdmp/admin\" \n" +
                 " at \"/MarkLogic/admin.xqy\"; \n" +
        "let $conf := admin:get-configuration() \n" +
        "for $forest in xdmp:database-forests(xdmp:database()) \n" +         
        "let $host_id := admin:forest-get-host($conf, $forest) \n" +
        "let $host_name := admin:host-get-name($conf, $host_id) \n" +
        "let $cnt := xdmp:estimate(cts:search(" + PATH_EXPRESSION_TEMPLATE + ", \n" +
        "                          cts:and-query(()), (), 0.0, $forest)) \n" +
        "return ($forest, $cnt, $host_name)";    
    */
	static final String SPLIT_QUERY_TEMPLATE =
		"xquery version \"1.0-ml\"; \n" + 
        "import module namespace admin = \"http://marklogic.com/xdmp/admin\" \n" +
                 " at \"/MarkLogic/admin.xqy\"; \n" +
        "let $conf := admin:get-configuration() \n" +
        "for $forest in xdmp:database-forests(xdmp:database()) \n" +         
        "let $host_id := admin:forest-get-host($conf, $forest) \n" +
        "let $host_name := admin:host-get-name($conf, $host_id) \n" +
        "let $cnt := xdmp:estimate(cts:search(fn:doc(), \n" +
        "                          cts:and-query(()), (), 0.0, $forest)) \n" +
        "return ($forest, $cnt, $host_name)";    
    /**
     * get server URI from the configuration.
     * 
     * @param conf job configuration
     * @return server URI
     * @throws URISyntaxException 
     */
    private URI getServerUri(Configuration conf) throws URISyntaxException {
		String user = conf.get(INPUT_USERNAME, "");
		String password = conf.get(INPUT_PASSWORD, "");
		String host = conf.get(INPUT_HOST, "");
		String port = conf.get(INPUT_PORT, "");
		
		String serverUriStr = 
			SERVER_URI_TEMPLATE.replace(USER_TEMPLATE, user)
			.replace(PASSWORD_TEMPLATE, password)
			.replace(HOST_TEMPLATE, host)
			.replace(PORT_TEMPLATE, port);
		return new URI(serverUriStr);
    }
    
    /**
     * get server URI leaving out the host from the configuration.
     * 
     * @param conf job configuration
     * @return server URI template
     */
    protected String getServerUriTemp(Configuration conf) {
    	String user = conf.get(INPUT_USERNAME, "");
		String password = conf.get(INPUT_PASSWORD, "");
		String port = conf.get(INPUT_PORT, "");
		return
			SERVER_URI_TEMPLATE.replace(USER_TEMPLATE, user)
			.replace(PASSWORD_TEMPLATE, password)
			.replace(PORT_TEMPLATE, port);
    }

	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
			InterruptedException {
		// get input from job configuration
		Configuration jobConf = jobContext.getConfiguration();
		long maxSplitSize;
		URI serverUri = null;
		String pathExpr;
		String nameSpace;
		float recordToFragRatio;
		String splitQuery;
		try {
			serverUri = getServerUri(jobConf);
			pathExpr = jobConf.get(PATH_EXPRESSION, "");
			nameSpace = jobConf.get(PATH_NAMESPACE, "");
			maxSplitSize = jobConf.getLong(MAX_SPLIT_SIZE, 
					DEFAULT_MAX_SPLIT_SIZE);
			recordToFragRatio = jobConf.getFloat(RECORD_TO_FRAGMENT_RATIO, 
					getDefaultRecordFragRatio());
			splitQuery = jobConf.get(SPLIT_QUERY, "");
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		} 
		
		// fetch data from server
		List<ForestSplit> forestSplits = null;
		Session session = null;
		ResultSequence result = null;
		if (splitQuery.isEmpty()) {
		    splitQuery = SPLIT_QUERY_TEMPLATE
	        .replace(PATH_EXPRESSION_TEMPLATE, pathExpr)
	        .replace(NAMESPACE_TEMPLATE, nameSpace);
		}
		if (LOG.isDebugEnabled()) {
		    LOG.debug("query: " + splitQuery);
		}
		try {
			ContentSource cs = ContentSourceFactory.newContentSource(serverUri);
			session = cs.newSession();
			AdhocQuery query = session.newAdhocQuery(splitQuery);
			RequestOptions options = new RequestOptions();
			options.setCacheResult(false);
			query.setOptions(options);
			result = session.submitRequest(query);
			
			int count = 0;
			while (result.hasNext()) {
				ResultItem item = result.next();
				if (forestSplits == null) {
					forestSplits = new ArrayList<ForestSplit>();
				}
				int index = count % 3;
				if (index == 0) {
					assert item.getItemType() == ItemType.XS_INTEGER;
					ForestSplit split = new ForestSplit();
					split.forestId = ((XSInteger)item.getItem()).asBigInteger();
					forestSplits.add(split);
				} else if (index == 1) {
					assert item.getItemType() == ItemType.XS_INTEGER;
					assert forestSplits.size() > 0;
					forestSplits.get(forestSplits.size() - 1).recordCount = 
						((XSInteger)item.getItem()).asLong();
				} else if (index == 2) {
					assert item.getItemType() == ItemType.XS_STRING;
					assert forestSplits.size() > 0;
					forestSplits.get(forestSplits.size() - 1).hostName = 
						((XSString)item.getItem()).asString();
				}
				count++;
			}
			LOG.info("Fetched " + forestSplits.size() + " splits.");
		} catch (XccConfigException e) {
			LOG.error(e);
			throw new IOException(e);
		} catch (RequestException e) {
			LOG.error(e);
			LOG.error("Query: " + splitQuery);
			throw new IOException(e);
		} finally {
			if (result != null) {
				result.close();
			}
			if (session != null) {
				try {
	                session.close();
                } catch (RequestException e) {
                	LOG.error(e);
                }
			}
		}
		
		// construct and return splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		if (forestSplits == null || forestSplits.isEmpty()) {
			return splits;
		}
		
		for (ForestSplit fsplit : forestSplits) {
			long recordCount = 
				(long) (fsplit.recordCount * recordToFragRatio);
			if (recordCount < maxSplitSize) {
				MarkLogicInputSplit split = 
					new MarkLogicInputSplit(0, recordCount, 
							fsplit.forestId, fsplit.hostName);
				splits.add(split);
			} else {
				long splitCount = recordCount / maxSplitSize;
				long remainder = recordCount % maxSplitSize;
				if (remainder != 0) {
					splitCount++;
				}
				long splitSize = recordCount / splitCount;
			    long remainingCount = recordCount;
			    while (remainingCount > 0) {
			    	long start = fsplit.recordCount - remainingCount;
			    	// assign length to max value when it is the last split,
			    	// since the record count is not accurate
			    	long length = remainingCount < maxSplitSize ? 
			    		   	      Long.MAX_VALUE : splitSize; 
			    	MarkLogicInputSplit split = 
						new MarkLogicInputSplit(start, length, fsplit.forestId,
								fsplit.hostName);
			    	splits.add(split);
			    	remainingCount -= length;
			    }
			}
		}
		LOG.info("Made " + splits.size() + " splits.");
		return splits;
	}
	
	/**
	 * A per-forest split of the result records.
	 * 
	 * @author jchen
	 */
	class ForestSplit {
		BigInteger forestId;
		String hostName;
		long recordCount;
	}

	public abstract float getDefaultRecordFragRatio();

}
