/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
 * MarkLogic-based InputFormat, taking a generic key and value class.
 * 
 * @author jchen
 */
public abstract class MarkLogicInputFormat<KEYIN, VALUEIN> extends InputFormat<KEYIN, VALUEIN> 
implements MarkLogicConstants {
	public static final Log LOG =
	    LogFactory.getLog(MarkLogicInputFormat.class);
	
	static final String SPLIT_QUERY_TEMPLATE =
		"xquery version \"1.0-ml\"; \n" + 
        "import module namespace admin = \"http://marklogic.com/xdmp/admin\" \n" +
                 " at \"/MarkLogic/admin.xqy\"; \n" +
        "let $conf := admin:get-configuration() \n" +
        "for $forest in xdmp:database-forests(xdmp:database()) \n" +         
        "let $host_id := admin:forest-get-host($conf, $forest) \n" +
        "let $host_name := admin:host-get-name($conf, $host_id) \n" +
        "let $cnt := xdmp:with-namespaces((" + NAMESPACE_TEMPLATE + 
        "), xdmp:estimate(cts:search(" + DOCUMENT_SELECTOR_TEMPLATE + ",\n" +
        "                          cts:and-query(()), (), 0.0, $forest))) \n" +
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
		String docSelector;
		String splitQuery;
		try {
			serverUri = getServerUri(jobConf);
			docSelector = jobConf.get(DOCUMENT_SELECTOR, "fn:collection()");	
			maxSplitSize = jobConf.getLong(MAX_SPLIT_SIZE, 
					DEFAULT_MAX_SPLIT_SIZE);
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
			Collection<String> nsCol = 
				jobConf.getStringCollection(PATH_NAMESPACE);
			StringBuilder buf = new StringBuilder();
			if (nsCol != null) {
				for (Iterator<String> nsIt = nsCol.iterator(); nsIt.hasNext();) {
					String ns = nsIt.next();
					buf.append('"').append(ns).append('"');
					if (nsIt.hasNext()) {
						buf.append(',');
					}
				}
			}
		    splitQuery = SPLIT_QUERY_TEMPLATE
		        .replace(DOCUMENT_SELECTOR_TEMPLATE, docSelector)
		        .replace(NAMESPACE_TEMPLATE, buf.toString()); 
		} 
		
		if (LOG.isDebugEnabled()) {
		    LOG.debug("Split query: " + splitQuery);
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
			LOG.info("Fetched " + forestSplits.size() + " forest splits.");
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
			if (fsplit.recordCount < maxSplitSize) {
				MarkLogicInputSplit split = 
					new MarkLogicInputSplit(0, fsplit.recordCount, 
							fsplit.forestId, fsplit.hostName);
				splits.add(split);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Added split " + split);
				}	
			} else {
				long splitCount = fsplit.recordCount / maxSplitSize;
				long remainder = fsplit.recordCount % maxSplitSize;
				if (remainder != 0) {
					splitCount++;
				}
				long splitSize = fsplit.recordCount / splitCount;
			    long remainingCount = fsplit.recordCount;
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
			    	if (LOG.isDebugEnabled()) {
						LOG.debug("Added split " + split);
					}
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
}
