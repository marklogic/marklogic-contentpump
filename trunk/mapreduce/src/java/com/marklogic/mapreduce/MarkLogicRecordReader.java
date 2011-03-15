package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * <K, MarkLogicRecord> key value pairs.
 * 
 * @author jchen
 */
public abstract class MarkLogicRecordReader<K> 
extends RecordReader<K, MarkLogicRecord> 
implements MarkLogicConstants {
	public static final Log LOG =
	    LogFactory.getLog(MarkLogicRecordReader.class);
	static final String FOREST_ID_TEMPLATE = "{forest_id}";
	static final String START_TEMPLATE = "{start}";
    static final String END_TEMPLATE = "{end}";
	static final String QUERY_TEMPLATE =
    	"xquery version \"1.0-ml\"; \n" + 
    	"(xdmp:eval('" + NAMESPACE_TEMPLATE + "fn:unordered(" + 
    	PATH_EXPRESSION_TEMPLATE + ")[" + START_TEMPLATE + " to " + 
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
	private int count;
	/**
	 * URI of the MarkLogic server with host to be filled in.
	 */
	private String serverUriTemp;
	/**
	 * Path expression specified in the job configuration.
	 */
	private String pathExpr;
	/**
	 * Namespace specified in the job configuration.
	 */
	private String nameSpace;
	/**
	 * Session to the MarkLogic server.
	 */
	private Session session;
	/**
	 * ResultSequence from the MarkLogic server.
	 */
	private ResultSequence result;

	/**
	 * Current value.
	 */
	private MarkLogicRecord currentValue;
	
	public MarkLogicRecordReader(String serverUri, String pathExpr, String nameSpace) {
		this.serverUriTemp = serverUri;
		this.pathExpr = pathExpr;
		this.nameSpace = nameSpace;
	}

	@Override
	public void close() throws IOException {
		try {
			if (result != null) {
				result.close();
			}
			if (session != null) {
				session.close();
			}
		} catch(RequestException e) {
			LOG.error(e);
			throw new IOException(e);
		}
	}

	@Override
	public MarkLogicRecord getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
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
		
		// generate the query
		long start = mlSplit.getStart() + 1;
		long end = start + mlSplit.getLength() - 1;
		String queryText = QUERY_TEMPLATE
		        .replace(PATH_EXPRESSION_TEMPLATE, pathExpr)
		        .replace(NAMESPACE_TEMPLATE, nameSpace)
		        .replace(FOREST_ID_TEMPLATE, mlSplit.getForestId().toString())
		        .replace(START_TEMPLATE, Long.toString(start))
	            .replace(END_TEMPLATE, Long.toString(end));	 
		
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
			setCurrentKey(item);
			currentValue = new MarkLogicRecord(item);
			count++;
			return true;
		} else {
			setCurrentKey(null);
			currentValue = null;
			return false;
		}
	}
	
    abstract protected void setCurrentKey(ResultItem item);
}
