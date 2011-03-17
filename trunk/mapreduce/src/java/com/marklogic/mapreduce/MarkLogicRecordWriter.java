package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * A RecordWriter that persists MarkLogicRecord to MarkLogic server.
 * 
 * @author jchen
 *
 */
public abstract class MarkLogicRecordWriter<K>
extends RecordWriter<K, MarkLogicNode> {
	public static final Log LOG =
	    LogFactory.getLog(MarkLogicRecordWriter.class);
	/**
	 * Server URI.
	 */
	private URI serverUri;
	/**
	 * Session to the MarkLogic server.
	 */
	private Session session;
	
	public MarkLogicRecordWriter(URI serverUri) {
		this.serverUri = serverUri;
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (session != null) {
			try {
	            session.close();
            } catch (RequestException e) {
            	LOG.error(e);
            }
		}
	}
	
	protected Session getSession() throws IOException {
		if (session == null) {
			// start a session
			try {
				ContentSource cs = ContentSourceFactory.newContentSource(serverUri);
			    session = cs.newSession(); 
			} catch (XccConfigException e) {
				e.printStackTrace();
				throw new IOException(e);
			}    
		}
		return session;
	}
}
