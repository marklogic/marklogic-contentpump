/*
 * Copyright (c) 2020 MarkLogic Corporation
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

import com.marklogic.xcc.exceptions.MLCloudRequestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentSource;

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
            isCopyColls, isCopyQuality, isCopyMeta, isCopyPerms, 
            effectiveVersion);
        boolean naked = meta.isNakedProps();
        if (sessions[sid] == null) {
            sessions[sid] = getSession(sid, false);
            queries[sid] = getAdhocQuery(sid);
        }
        boolean committed = false;
        if (!naked) {
            opt.setFormat(doc.getContentType().getDocumentFormat());
            addValue(uri, value, sid, opt, effectiveVersion<PROPS_MIN_VERSION?null:meta.getProperties());
            pendingURIs[sid].add((DocumentURI)key.clone());
            if (++counts[sid] == batchSize) {
                commitRetry = 0;
                commitSleepTime = MIN_SLEEP_TIME;
                while (commitRetry < commitRetryLimit) {
                    committed = false;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getFormattedBatchId() +
                            "Retrying committing batch , attempts: " +
                            commitRetry + "/" + MAX_RETRIES);
                    }
                    queries[sid].setNewVariables(uriName, uris[sid]);
                    queries[sid].setNewVariables(contentName, values[sid]);
                    queries[sid].setNewVariables(optionsName, optionsVals[sid]);
                    try {
                        insertBatch(sid, uris[sid], values[sid], optionsVals[sid]);
                    } catch (Exception e) {}
                    stmtCounts[sid]++;
                    //reset forest index for statistical
                    if (countBased) {
                        sfId = -1;
                    }
                    counts[fId] = 0;

                    if (needCommit && stmtCounts[sid] == txnSize) {
                        try {
                            commit(sid);
                            if (commitRetry > 0) {
                                LOG.info(getFormattedBatchId() +
                                    "Retrying committing batch is successful");
                            }
                        } catch (Exception e) {
                            boolean isRetryable = true;
                            LOG.warn("Failed committing transaction.");
                            if (e instanceof MLCloudRequestException){
                                isRetryable = ((MLCloudRequestException)e).isRetryable();
                                LOG.warn(getFormattedBatchId() +
                                    "MLCloudRequestException:" + e.getMessage());
                            } else {
                                LOG.warn(getFormattedBatchId() +
                                    "Exception:" + e.getMessage());
                            }
                            if (isRetryable && needCommitRetry() &&
                                (++commitRetry < commitRetryLimit)) {
                                LOG.warn(getFormattedBatchId() + "Failed during committing");
                                handleCommitExceptions(sid);
                                commitSleepTime = sleep(commitSleepTime);
                                stmtCounts[sid] = 0;
                                sessions[sid] = getSession(sid, true);
                                continue;
                            } else if (needCommitRetry()) {
                                LOG.error(getFormattedBatchId() +
                                    "Exceeded max commit retry, batch failed permanently");
                            }
                            failed += commitUris[sid].size();
                            for (DocumentURI failedUri : commitUris[sid]) {
                                LOG.error(getFormattedBatchId() +
                                    "Document failed permanently: " + failedUri);
                            }
                            handleCommitExceptions(sid);
                        } finally {
                            stmtCounts[sid] = 0;
                            committed = true;
                        }
                    }
                    break;
                }
                batchId++;
                pendingURIs[sid].clear();
            }
        }

        if (isCopyProps && meta.getProperties() != null &&
                (effectiveVersion < PROPS_MIN_VERSION || naked)) {
            boolean suc = DatabaseContentWriter.setDocumentProperties(uri, 
                    meta.getProperties(),
                    isCopyPerms&&naked?meta.getPermString():null,
                    isCopyColls&&naked?meta.getCollectionString():null,
                    isCopyQuality&&naked?meta.getQualityString():null,
                    isCopyMeta&&naked?meta.getMeta():null, sessions[sid]);
            stmtCounts[sid]++;
            if (suc && naked) {
                if (needCommit) {
                    commitUris[sid].add(key);
                } else {
                    succeeded++;
                }
            } else if (!suc && naked) {
                failed++;
            }
        }

        if (needCommit && stmtCounts[sid] >= txnSize) {
            try {
                commit(sid);
            } catch (Exception e) {
                LOG.warn(getFormattedBatchId() +
                    "Failed committing transaction: " + e.getMessage());
                handleCommitExceptions(sid);
            }
            stmtCounts[sid] = 0;
            committed = true;
        }

        if ((!fastLoad) && ((!needCommit) || committed)) { 
            // rotate to next host and reset session
            hostId = (hostId + 1)%forestIds.length;
            sessions[0] = null;
            queries[0] = null;
        }
    }
}
