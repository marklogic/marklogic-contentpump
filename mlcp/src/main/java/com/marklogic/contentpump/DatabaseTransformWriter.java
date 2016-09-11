/*
 * Copyright 2003-2016 MarkLogic Corporation
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

import com.marklogic.contentpump.utilities.TransformHelper;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
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
    private AdhocQuery[] queries;
    
    public DatabaseTransformWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad,
        AssignmentManager am) {
        super(conf, forestSourceMap, fastLoad, am);
        moduleUri = conf.get(ConfigConstants.CONF_TRANSFORM_MODULE);
        functionNs = conf.get(ConfigConstants.CONF_TRANSFORM_NAMESPACE, "");
        functionName = conf.get(ConfigConstants.CONF_TRANSFORM_FUNCTION,
            "transform");
        functionParam = conf.get(ConfigConstants.CONF_TRANSFORM_PARAM, "");
        queries = new AdhocQuery[sessions.length];
    }

    @Override
    public void write(DocumentURI key, VALUE value) throws IOException,
        InterruptedException {
        int fId = 0;
        String uri = InternalUtilities.getUriWithOutputDir(key, outputDir);
        String csKey;
        if (fastLoad) {
            if(!countBased) {
                // placement for legacy or bucket
                fId = am.getPlacementForestIndex(key);
                sfId = fId;
            } else {
                if (sfId == -1) {
                    sfId = am.getPlacementForestIndex(key);
                }
                fId = sfId;
            }
            csKey = forestIds[fId];
        } else {
            csKey = forestIds[hostId];
        }
        int sid = fId;

        DocumentMetadata meta = null;
        DatabaseDocumentWithMeta doc = (DatabaseDocumentWithMeta) value;
        meta = doc.getMeta();
        ContentCreateOptions opt = newContentCreateOptions(meta);
        if (sessions[sid] == null) {
            sessions[sid] = getSession(csKey);
            queries[sid] = getAdhocQuery(sid);
        }
        if (!meta.isNakedProps()) {
            opt.setFormat(doc.getContentType().getDocumentFormat());
            AdhocQuery qry = 
                    TransformHelper.getTransformInsertQryMLDocWithMeta(conf, 
                            queries[sid], moduleUri, functionNs, functionName, 
                            functionParam, uri, doc, opt);
            try {
                sessions[sid].submitRequest(qry);
                stmtCounts[sid]++;
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
            } catch (RequestServerException e) {
                // log error and continue on RequestServerException
                if (e instanceof XQueryException) {
                    LOG.error(((XQueryException) e).getFormatString());
                } else {
                    LOG.error(e.getMessage());
                }
                LOG.warn("Failed document " + key);
                failed++;
            } catch (RequestException e) {
                if (sessions[sid] != null) {
                    sessions[sid].close();
                }
                if (countBased) {
                    rollbackCount(sid);
                }
                throw new IOException(e);
            }
            if (needCommit) {
                commitUris[sid].add(key);
            } else {
                succeeded++;
            } 
        }
        if (isCopyProps && meta.getProperties() != null) {
            boolean naked = meta.isNakedProps();
            setDocumentProperties(uri, meta.getProperties(),
                    isCopyPerms&&naked?meta.getPermString():null,
                    isCopyColls&&naked?meta.getCollectionString():null,
                    isCopyQuality&&naked?meta.getQualityString():null, 
                    isCopyMeta&&naked?meta.getMeta():null, sessions[sid]);
            stmtCounts[sid]++;
        }
        boolean committed = false;
        if (stmtCounts[sid] >= txnSize && needCommit) {
            commit(sid);
            stmtCounts[sid] = 0;
            commitUris[sid].clear();
            committed = true;
        }
        if ((!fastLoad) && ((!needCommit) || committed)) { 
            // rotate to next host and reset session
            hostId = (hostId + 1)%forestIds.length;
            sessions[0] = null;
        }
    }

    @Override
    protected Session getSession(String forestId) {
        TransactionMode mode = TransactionMode.AUTO;
        if (txnSize > 1) {
            mode = TransactionMode.UPDATE;
        }
        return getSession(forestId, mode);
    }
    
    protected AdhocQuery getAdhocQuery(int sid) {
        String qs = TransformHelper.constructQryString(moduleUri, functionNs,
                functionName, functionParam);
        AdhocQuery q = sessions[sid].newAdhocQuery(qs);
        RequestOptions rOptions = new RequestOptions();
        rOptions.setDefaultXQueryVersion("1.0-ml");
        q.setOptions(rOptions);
        return q;
    }

}
