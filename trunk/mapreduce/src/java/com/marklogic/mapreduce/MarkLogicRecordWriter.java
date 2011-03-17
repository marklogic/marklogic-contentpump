package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * A RecordWriter that persists MarkLogicRecord to MarkLogic server.
 * 
 * @author jchen
 *
 */
public abstract class MarkLogicRecordWriter<K>
extends RecordWriter<K, MarkLogicNode> {

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
			session.close();
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
