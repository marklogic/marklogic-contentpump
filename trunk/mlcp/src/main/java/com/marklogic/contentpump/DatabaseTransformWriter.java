/*
 * Copyright 2003-2015 MarkLogic Corporation
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
import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
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
        String forestId = ContentOutputFormat.ID_PREFIX;
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
            forestId = forestIds[fId];
        }
        int sid = fId;

        try {
            DocumentMetadata meta = null;
            DatabaseDocumentWithMeta doc = (DatabaseDocumentWithMeta) value;
            meta = doc.getMeta();
            newContentCreateOptions(meta);
            boolean isCopyProps = conf.getBoolean(
                ConfigConstants.CONF_COPY_PROPERTIES, true);
            if (sessions[sid] == null) {
                sessions[sid] = getSession(forestId);
            }
            if(queries[sid] == null) {
                queries[sid] = getAdhocQuery(sid);
            }
            if (!meta.isNakedProps()) {
                options.setFormat(doc.getContentType().getDocumentFormat());
                AdhocQuery qry = TransformHelper
                    .getTransformInsertQryMLDocWithMeta(conf, queries[sid],
                        moduleUri, functionNs, functionName, functionParam,
                        uri, doc, options);
                sessions[sid].submitRequest(qry);
                stmtCounts[sid]++;
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
            }
            
            if (isCopyProps && meta.getProperties() != null) {
                setDocumentProperties(uri, meta.getProperties(), sessions[sid]);
                stmtCounts[sid]++;
            }

            if (stmtCounts[sid] >= txnSize && 
                sessions[sid].getTransactionMode() == TransactionMode.UPDATE) {
                sessions[sid].commit();
                stmtCounts[sid] = 0;
            }
        } catch (RequestServerException e) {
            // log error and continue on RequestServerException
            if (e instanceof XQueryException) {
                LOG.warn(((XQueryException) e).getFormatString());
            } else {
                LOG.warn(e.getMessage());
            }
        } catch (RequestException e) {
            if (sessions[sid] != null) {
                sessions[sid].close();
            }
            if (countBased) {
                rollbackDocCount(sid);
            }
            throw new IOException(e);
        }
    }

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
