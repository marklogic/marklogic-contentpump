/*
 * Copyright 2003-2017 MarkLogic Corporation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;

/**
 * DatabaseContentWriter that does server-side transform and insert
 * @author ali
 *
 * @param <VALUE>
 */
public class DatabaseTransformWriter<VALUE> extends
    TransformWriter<VALUE> implements ConfigConstants {
    public static final Log LOG = 
            LogFactory.getLog(DatabaseTransformWriter.class);
    
    protected boolean isCopyProps;
    protected boolean isCopyPerms;
    
    public DatabaseTransformWriter(Configuration conf,
            Map<String, ContentSource> hostSourceMap, boolean fastLoad,
            AssignmentManager am) {
        super(conf, hostSourceMap, fastLoad, am);

        isCopyProps = conf.getBoolean(CONF_COPY_PROPERTIES, true);
        isCopyPerms = conf.getBoolean(CONF_COPY_PERMISSIONS, true);
    }
    
    @Override
    public void write(DocumentURI key, VALUE value) throws IOException,
        InterruptedException {
        int fId = 0;
        String uri = InternalUtilities.getUriWithOutputDir(key, outputDir);
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
        }
        int sid = fId;

        DatabaseDocumentWithMeta doc = (DatabaseDocumentWithMeta) value;
        DocumentMetadata meta = doc.getMeta();
        ContentCreateOptions opt = 
            DatabaseContentWriter.newContentCreateOptions(meta, options, 
            isCopyColls, isCopyQuality, isCopyMeta, isCopyPerms, isCopyProps,
            effectiveVersion);
        boolean naked = meta.isNakedProps();
        if (sessions[sid] == null) {
            sessions[sid] = getSession(sid, false);
            queries[sid] = getAdhocQuery(sid);
        }
        if (!naked) {
            opt.setFormat(doc.getContentType().getDocumentFormat());
            addValue(uri, value, sid, opt);
            pendingURIs[sid].add((DocumentURI)key.clone());
            if (++counts[sid] == batchSize) {
                queries[sid].setNewVariables(uriName, uris[sid]);
                queries[sid].setNewVariables(contentName, values[sid]);
                queries[sid].setNewVariables(optionsName, optionsVals[sid]);
                insertBatch(sid,uris[sid],values[sid],optionsVals[sid]);
                stmtCounts[sid]++;
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
                if (needCommit) {
                    commitUris[sid].addAll(pendingURIs[sid]);
                } else {
                    succeeded += pendingURIs[sid].size();
                }
                pendingURIs[sid].clear();
            }
        }
        if (isCopyProps && meta.getProperties() != null && naked) {
            boolean suc = DatabaseContentWriter.setDocumentProperties(uri, 
                    meta.getProperties(),
                    isCopyPerms?meta.getPermString():null,
                    isCopyColls?meta.getCollectionString():null,
                    isCopyQuality?meta.getQualityString():null,
                    isCopyMeta?meta.getMeta():null, sessions[sid]);
            stmtCounts[sid]++;
            if (suc) {
                if (needCommit) {
                    commitUris[sid].add(key);
                } else {
                    succeeded++;
                }
            } else if (!suc) {
                failed++;
            }
        }
        
        boolean committed = false;
        if (stmtCounts[sid] >= txnSize && needCommit) {
            commit(sid);
            stmtCounts[sid] = 0;
            committed = true;
        }
        if ((!fastLoad) && ((!needCommit) || committed)) { 
            // rotate to next host and reset session
            hostId = (hostId + 1)%forestIds.length;
            sessions[0] = null;
        }
    }

    protected Session getSession(int fId, boolean nextReplica) {
        TransactionMode mode = TransactionMode.AUTO;
        if (txnSize > 1) {
            mode = TransactionMode.UPDATE;
        }
        return getSession(fId, nextReplica, mode);
    }
}
