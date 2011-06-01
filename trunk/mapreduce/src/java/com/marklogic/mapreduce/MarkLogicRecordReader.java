/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
/**
 * A RecordReader that fetches data from MarkLogic server and generates 
 * <K, V> key value pairs.
 * 
 * @author jchen
 * 
 * @param <KEYIN, VALUEIN>
 */
public abstract class MarkLogicRecordReader<KEYIN, VALUEIN> 
extends RecordReader<KEYIN, VALUEIN>
implements MarkLogicConstants {

	public static final Log LOG =
	    LogFactory.getLog(MarkLogicRecordReader.class);
    
	/**
	 * Input split for this record reader
	 */
	private MarkLogicInputSplit mlSplit;
	/**
	 * Count of records fetched
	 */
	private long count;
	/**
	 * Session to the MarkLogic server.
	 */
	private Session session;
	/**
	 * ResultSequence from the MarkLogic server.
	 */
	private ResultSequence result;
	/**
	 * Job configuration.
	 */
	private Configuration conf;
	/**
	 * Total expected count of the records in a split.
	 */
	private float length;
	
	public MarkLogicRecordReader(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void close() throws IOException {
		if (result != null) {
			result.close();
		}
		if (session != null) {
	        session.close();
		}
	}

	public Configuration getConf() {
    	return conf;
    }

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return count/length;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		mlSplit = (MarkLogicInputSplit)split;
		count = 0;
		
		// construct the server URI
		URI serverUri;
		try {
			String[] hostNames = mlSplit.getLocations();
			assert hostNames != null && hostNames.length >= 1;
			serverUri = InternalUtilities.getInputServerUri(conf, hostNames[0]);
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		} 
		
		// get job config properties
		boolean advancedMode = 
			conf.get(INPUT_MODE, BASIC_MODE).equals(ADVANCED_MODE);
		
		// initialize the total length
		float recToFragRatio = conf.getFloat(RECORD_TO_FRAGMENT_RATIO, 
				getDefaultRatio());
		length = mlSplit.getLength() * recToFragRatio;
		
		// generate the query
		long start = mlSplit.getStart() + 1;
		long end = mlSplit.getLength() == Long.MAX_VALUE ? 
				   mlSplit.getLength() : start + mlSplit.getLength() - 1;
				  
		String forestId = mlSplit.getForestId().toString();
		StringBuilder buf = new StringBuilder();
		if (advancedMode) {
			String userQuery = conf.get(INPUT_QUERY, "");
			buf.append("xquery version \"1.0-ml\"; \n(xdmp:eval('"); 
	    	buf.append(userQuery);
	    	buf.append("',  (), \n  <options xmlns=\"xdmp:eval\"> <database>");
	  		buf.append(forestId);
	  		buf.append("</database> \n  </options>))[");
	  		buf.append(Long.toString(start));
	  		buf.append(" to ");
	  		buf.append(Long.toString(end));
	  		buf.append("]");
		} else {
			String docExpr = conf.get(DOCUMENT_SELECTOR, "fn:collection()");
			String subExpr = conf.get(SUBDOCUMENT_EXPRESSION, "");
			Collection<String> nsCol = conf.getStringCollection(PATH_NAMESPACE);
			
			buf.append("xquery version \"1.0-ml\"; \n");
			buf.append("(xdmp:eval('xdmp:with-namespaces(("); 
			if (nsCol != null) {
				for (Iterator<String> nsIt = nsCol.iterator(); nsIt.hasNext();) {
					String ns = nsIt.next();
					buf.append('"').append(ns).append('"');
					if (nsIt.hasNext()) {
						buf.append(',');
					}
				}
			}
			buf.append("),fn:unordered(fn:unordered(");
			buf.append(docExpr);
    	    buf.append(")[");
    	    buf.append(Long.toString(start));
    	    buf.append(" to ");
    	    buf.append(Long.toString(end));
    	    buf.append("]");
    	    buf.append(subExpr);
    	    buf.append("))', (), \n <options xmlns=\"xdmp:eval\"> <database>");
  		    buf.append(forestId);
  		    buf.append("</database> \n  </options>))");
		}
		
		String queryText = buf.toString();
		if (LOG.isDebugEnabled()) {
			LOG.debug(queryText);
		}
		
		// set up a connection to the server
		try {
			ContentSource cs = InternalUtilities.getInputContentSource(conf, 
					serverUri);
		    session = cs.newSession(); 
		    AdhocQuery query = session.newAdhocQuery(queryText);
		    RequestOptions options = new RequestOptions();
			options.setCacheResult(false);
			query.setOptions(options);
		    result = session.submitRequest(query);
		} catch (XccConfigException e) {
			LOG.error(e);
			throw new IOException(e);
		} catch (RequestException e) {
			LOG.error("Query: " + queryText);
			LOG.error(e);
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (result != null && result.hasNext()) {
			ResultItem item = result.next();
			count++;
			return nextResult(item);
		} else {
			endOfResult();
			return false;
		}
	}

	abstract protected void endOfResult();

	abstract protected boolean nextResult(ResultItem result);
	
	protected abstract float getDefaultRatio();

	public long getCount() {
		return count;
	}
}
