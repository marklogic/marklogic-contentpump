package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

/**
 * MarkLogicRecordWriter that sets a property for a document.
 * 
 * @author jchen
 */
public class PropertyWriter 
extends MarkLogicRecordWriter<DocumentURI, MarkLogicNode> {

	public static final Log LOG =
	    LogFactory.getLog(PropertyWriter.class);
	
	public PropertyWriter(URI serverUri) {
		super(serverUri);
	}

	@Override
	public void write(DocumentURI uri, MarkLogicNode record)
			throws IOException, InterruptedException {
		// construct query
		String recordString = record == null ? "()" : record.toString();
		StringBuilder buf = new StringBuilder();
		buf.append("xquery version \"1.0-ml\"; \n");
		buf.append("xdmp:document-set-property( \n\"");
		buf.append(uri.getUnparsedUri());
		buf.append("\", ");
		buf.append(recordString);
		buf.append(")");
		String query = buf.toString();
		if (LOG.isDebugEnabled()) {
			LOG.debug(query);
		}

        // execute query
		Session session = getSession();
		try {
			AdhocQuery request = session.newAdhocQuery(query);
			session.submitRequest(request);
		} catch (RequestException e) {	
			LOG.error(e);
			LOG.error(query);
			//throw new IOException(e);
		}
	}

}
