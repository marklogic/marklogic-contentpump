package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

/**
 * MarkLogicRecordWriter that sets a property for a document.
 * 
 * @author jchen
 */
public class PropertyWriter 
extends MarkLogicRecordWriter<DocumentURI, MarkLogicNode> 
implements MarkLogicConstants {

	public static final Log LOG =
	    LogFactory.getLog(PropertyWriter.class);
	private PropertyOpType opType;
	
	public PropertyWriter(URI serverUri, Configuration conf) {
		super(serverUri, conf);
		String propOpType = conf.get(PROPERTY_OPERATION_TYPE, 
				DEFAULT_PROPERTY_OPERATION_TYPE);
		opType = PropertyOpType.valueOf(propOpType);
	}

	@Override
	public void write(DocumentURI uri, MarkLogicNode record)
			throws IOException, InterruptedException {
		// construct query
		String recordString = record == null ? "()" : record.toString();
		String query = opType.getQuery(uri, recordString);
		if (LOG.isDebugEnabled()) {
			LOG.debug(query);
		}

        // execute query
		Session session = getSession();
		try {
			AdhocQuery request = session.newAdhocQuery(query);
			session.submitRequest(request);
			commitIfNecessary();
		} catch (RequestException e) {	
			LOG.error(e);
			LOG.error(query);
			throw new IOException(e);
		}
	}

}
