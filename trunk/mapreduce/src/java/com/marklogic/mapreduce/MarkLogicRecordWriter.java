/*
 * Copyright 2003-2014 MarkLogic Corporation

 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.utilities.InternalUtilities;
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
     * Session to the MarkLogic server.
     */
    private Session session;
    private int count = 0;
    protected Configuration conf;
    protected int txnSize;
    protected String hostName;
    
    public MarkLogicRecordWriter(Configuration conf, String hostName) {
        this.hostName = hostName;
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
                        conf, hostName);
                session = cs.newSession();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Connect to " + session.getConnectionUri().getHost());
                }
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
