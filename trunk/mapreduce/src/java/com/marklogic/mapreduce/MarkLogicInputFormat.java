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
import org.apache.hadoop.io.DefaultStringifier;
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


    /**
     * Get input splits.
     * @param jobContext job context
     * @return list of input splits    
     */
	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
			InterruptedException {
		// get input from job configuration
		Configuration jobConf = jobContext.getConfiguration();
		long maxSplitSize;

		String docSelector;
		String splitQuery;
		docSelector = jobConf.get(DOCUMENT_SELECTOR, "fn:collection()");	
		maxSplitSize = jobConf.getLong(MAX_SPLIT_SIZE, 
				DEFAULT_MAX_SPLIT_SIZE);
		splitQuery = jobConf.get(SPLIT_QUERY, "");
		
		// fetch data from server
		List<ForestSplit> forestSplits = null;
		Session session = null;
		ResultSequence result = null;
		
		if (splitQuery.isEmpty()) {
			Collection<String> nsCol = 
				jobConf.getStringCollection(PATH_NAMESPACE);
			StringBuilder buf = new StringBuilder();
			buf.append("xquery version \"1.0-ml\"; \n");
			buf.append("import module namespace admin = ");
			buf.append("\"http://marklogic.com/xdmp/admin\" \n");
            buf.append(" at \"/MarkLogic/admin.xqy\"; \n");
            buf.append("let $conf := admin:get-configuration() \n");
            buf.append("for $forest in xdmp:database-forests(xdmp:database())\n");        
            buf.append("let $host_id := admin:forest-get-host($conf, $forest)\n");
            buf.append("let $host_name := admin:host-get-name($conf, $host_id)\n");
            buf.append("let $cnt := xdmp:with-namespaces((");
			if (nsCol != null) {
				for (Iterator<String> nsIt = nsCol.iterator(); nsIt.hasNext();) {
					String ns = nsIt.next();
					buf.append('"').append(ns).append('"');
					if (nsIt.hasNext()) {
						buf.append(',');
					}
				}
			}
			buf.append("), xdmp:estimate(cts:search(");
			buf.append(docSelector);
			buf.append(",\n    cts:and-query(()), (), 0.0, $forest))) \n");
            buf.append("return ($forest, $cnt, $host_name)"); 
            splitQuery = buf.toString();
		} 		
		
		if (LOG.isDebugEnabled()) {
		    LOG.debug("Split query: " + splitQuery);
		}
		try {
			ContentSource cs = InternalUtilities.getInputContentSource(jobConf);
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
		} catch (URISyntaxException e) {
			LOG.error(e);
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
