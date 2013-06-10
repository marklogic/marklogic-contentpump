/*
 * Copyright 2003-2013 MarkLogic Corporation
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
package com.marklogic.contentpump;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RequestServerException;
import com.marklogic.xcc.exceptions.XQueryException;

/**
 * DatabaseContentWriter that does server-side transform and insert
 * @author ali
 *
 * @param <VALUE>
 */
public class DatabaseTransformWriter<VALUE> extends
    DatabaseContentWriter<VALUE> {
    private String moduleUri;
    private String functionNs;
    private String functionName;
    private String functionParam;

    public DatabaseTransformWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad,
        AssignmentManager am) {
        super(conf, forestSourceMap, fastLoad, am);
        moduleUri = conf.get(ConfigConstants.CONF_TRANSFORM_MODULE);
        functionNs = conf.get(ConfigConstants.CONF_TRANSFORM_NAMESPACE, "");
        functionName = conf.get(ConfigConstants.CONF_TRANSFORM_FUNCTION,
            "transform");
        functionParam = conf.get(ConfigConstants.CONF_TRANSFORM_PARAM, "");
    }

    @Override
    public void write(DocumentURI key, VALUE value) throws IOException,
        InterruptedException {
        int fId = 0;
        String uri = getUriWithOutputDir(key, outputDir);
        String forestId = ContentOutputFormat.ID_PREFIX;
        if (fastLoad) {
            fId = am.getPlacementForestIndex(key);
            forestId = forestIds[fId];
        } 

        try {
            DocumentMetadata meta = null;
            MarkLogicDocumentWithMeta doc = (MarkLogicDocumentWithMeta) value;
            meta = doc.getMeta();
            newContentCreateOptions(meta);
            boolean isCopyProps = conf.getBoolean(
                ConfigConstants.CONF_COPY_PROPERTIES, true);
            if (!meta.isNakedProps()) {
                options.setFormat(doc.getContentType().getDocumentFormat());
                if (sessions[fId] == null) {
                    sessions[fId] = getSession(forestId);
                }
                AdhocQuery qry = TransformHelper
                    .getTransformInsertQryMLDocWithMeta(conf, sessions[fId],
                        moduleUri, functionNs, functionName, functionParam,
                        uri, doc, options);
                sessions[fId].submitRequest(qry);
                stmtCounts[fId]++;
                // update fragment count for statistical
                if (needFrmtCount) {
                    updateFrmtCount(fId, 1);
                }
            }
            
            if (isCopyProps && meta.getProperties() != null) {
                setDocumentProperties(uri, meta.getProperties(), sessions[fId]);
                stmtCounts[fId]++;
            }

            if (stmtCounts[fId] >= txnSize && 
                sessions[fId].getTransactionMode() == TransactionMode.UPDATE) {
                sessions[fId].commit();
                stmtCounts[fId] = 0;

                if (needFrmtCount) {
                    frmtCount[fId] = 0;
                }
            }
        } catch (RequestServerException e) {
            // log error and continue on RequestServerException
            if (e instanceof XQueryException) {
                LOG.warn(((XQueryException) e).getFormatString());
            } else {
                LOG.warn(e.getMessage());
            }
        } catch (RequestException e) {
            if (sessions[fId] != null) {
                sessions[fId].close();
            }
            if (needFrmtCount) {
                rollbackDocCount(fId);
            }
            throw new IOException(e);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
        for (int i = 0; i < sessions.length; i++) {
            if (sessions[i] == null)
                continue;
            if (stmtCounts[i] > 0 && 
                sessions[i].getTransactionMode() == TransactionMode.UPDATE) {
                try {
                    sessions[i].commit();
                } catch (RequestException e) {
                    LOG.error(e);
                    if (needFrmtCount) {
                        rollbackDocCount(i);
                    }
                    throw new IOException(e);
                } finally {
                    sessions[i].close();
                }
            } else {
                sessions[i].close();
            }

        }
    }

    @Override
    public int getTransactionSize(Configuration conf) {
        if (conf.get(TXN_SIZE) != null) {
            int txnSize = conf.getInt(TXN_SIZE, 0);
            return txnSize <= 0 ? 1 : txnSize;
        }
        return 10;
    }

    protected Session getSession(String forestId) {
        Session session = null;
        ContentSource cs = forestSourceMap.get(forestId);
        if (fastLoad) {
            session = cs.newSession(forestId);

        } else {
            session = cs.newSession();
        }
        if (txnSize > 1) {
            session.setTransactionMode(TransactionMode.UPDATE);
        }
        return session;
    }

}
