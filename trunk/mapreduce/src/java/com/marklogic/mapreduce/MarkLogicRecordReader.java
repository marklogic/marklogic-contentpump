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
import com.marklogic.xcc.ContentSourceFactory;
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
	static final String FOREST_ID_TEMPLATE = "{forest_id}";
	static final String START_TEMPLATE = "{start}";
    static final String END_TEMPLATE = "{end}";
	static final String BASIC_QUERY_TEMPLATE =
    	"xquery version \"1.0-ml\"; \n" + 
    	"(xdmp:eval('xdmp:with-namespaces((" + NAMESPACE_TEMPLATE +
    	"),fn:unordered(" +
    	PATH_EXPRESSION_TEMPLATE + "))[" + START_TEMPLATE + " to " + 
    	END_TEMPLATE + "] ',  (), \n" +
  		"  <options xmlns=\"xdmp:eval\"> <database>" + FOREST_ID_TEMPLATE +
  		"</database> \n" +
  		"  </options>))";
    static final String ADV_QUERY_TEMPLATE =
    	"xquery version \"1.0-ml\"; \n" + 
    	"(xdmp:eval('" + QUERY_TEMPLATE + "[" + START_TEMPLATE + " to " + 
    	END_TEMPLATE + "] ',  (), \n" +
  		"  <options xmlns=\"xdmp:eval\"> <database>" + FOREST_ID_TEMPLATE +
  		"</database> \n" +
  		"  </options>))";
    
	/**
	 * Input split for this record reader
	 */
	private MarkLogicInputSplit mlSplit;
	/**
	 * Count of records fetched
	 */
	private long count;
	/**
	 * URI of the MarkLogic server with host to be filled in.
	 */
	private String serverUriTemp;
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
	
	public MarkLogicRecordReader(Configuration conf, String serverUriTemp) {
		this.conf = conf;
		this.serverUriTemp = serverUriTemp;
	}

	@Override
	public void close() throws IOException {
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

	public Configuration getConf() {
    	return conf;
    }

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return count/(float)mlSplit.getLength();
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
			assert hostNames != null && hostNames.length == 1;
			String serverUriStr = serverUriTemp.replace(
					MarkLogicInputFormat.HOST_TEMPLATE, hostNames[0]);
			serverUri = new URI(serverUriStr);
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		} 
		
		// get job config properties
		boolean advancedMode = 
			conf.get(INPUT_MODE, BASIC_MODE).equals(ADVANCED_MODE);
		String userQuery = "";
		String pathExpr = "";
		String nameSpace = "";
		if (advancedMode) {
			userQuery = conf.get(INPUT_QUERY);
		} else {
			pathExpr = conf.get(PATH_EXPRESSION);
			Collection<String> nsCol = conf.getStringCollection(PATH_NAMESPACE);
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
			nameSpace = buf.toString();
		}
		
		// generate the query
		long start = mlSplit.getStart() + 1;
		long end = start + mlSplit.getLength() - 1;
		String queryText;
		if (advancedMode) {
			queryText = ADV_QUERY_TEMPLATE
				.replace(QUERY_TEMPLATE, userQuery)
		        .replace(FOREST_ID_TEMPLATE, mlSplit.getForestId().toString())
		        .replace(START_TEMPLATE, Long.toString(start))
	            .replace(END_TEMPLATE, Long.toString(end));
		} else {
			queryText = BASIC_QUERY_TEMPLATE
				.replace(PATH_EXPRESSION_TEMPLATE, pathExpr)
		        .replace(NAMESPACE_TEMPLATE, nameSpace)
		        .replace(FOREST_ID_TEMPLATE, mlSplit.getForestId().toString())
		        .replace(START_TEMPLATE, Long.toString(start))
	            .replace(END_TEMPLATE, Long.toString(end));
		}		        	 
		
		// set up a connection to the server
		try {
			ContentSource cs = ContentSourceFactory.newContentSource(serverUri);
		    Session session = cs.newSession(); 
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

	public long getCount() {
		return count;
	}
}
