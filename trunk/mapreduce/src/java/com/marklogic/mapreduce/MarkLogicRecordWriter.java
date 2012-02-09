/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * A RecordWriter that persists MarkLogicRecord to MarkLogic server.
 * 
 * @author jchen
 *
 */
public abstract class MarkLogicRecordWriter<KEYOUT, VALUEOUT>
extends RecordWriter<KEYOUT, VALUEOUT> implements MarkLogicConstants {
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
    private int count = 0;
    private Configuration conf;
    protected int txnSize;
    
    public MarkLogicRecordWriter(URI serverUri, Configuration conf) {
        this.serverUri = serverUri;
        this.conf = conf;
        this.txnSize = getTransactionSize(conf);
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        if (session != null) {
            try {
                if (count > 0 && txnSize > 1) {
                    session.commit();
                }
                session.close();
            } catch (RequestException e) {
                LOG.error(e);
            }
        }
    }
    
    /**
     * Get the session for this writer.  One writer only maintains one session.
     * 
     * @return Session for this writer.
     * @throws IOException
     */
    protected Session getSession() throws IOException {
        if (session == null) {
            // start a session
            try {
                ContentSource cs = InternalUtilities.getOutputContentSource(
                        conf, serverUri);
                session = cs.newSession();
                if (txnSize > 1) {
                    session.setTransactionMode(TransactionMode.UPDATE);
                }
            } catch (XccConfigException e) {
                LOG.error("Error creating a new session: ", e);
                throw new IOException(e);
            }    
        }
        return session;
    }
    
    protected void commitIfNecessary() throws RequestException {
        if (++count == txnSize && txnSize > 1) {
            session.commit();
            count = 0;
        }
    }
       
    public int getTransactionSize(Configuration conf) {
        return conf.getInt(TXN_SIZE, 1000);
    }
}
