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
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentPumpStats;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.ContentWriter;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;
import com.marklogic.xcc.exceptions.RequestException;

/**
 * MarkLogicRecordWriter that can 
 * 1) insert content from Archive to MarkLogic Server
 * 2) copy content from source MarkLogic Server to destination MarkLogic Server
 * 
 * @author ali
 * 
 */
public class DatabaseContentWriter<VALUE> extends
    ContentWriter<VALUE> implements MarkLogicConstants {
    public static final Log LOG = LogFactory
        .getLog(DatabaseContentWriter.class);

    private URIMetadata[][] metadatas;
    
    protected boolean isCopyProps;
    protected boolean isCopyColls;
    protected boolean isCopyPerms;
    protected boolean isCopyQuality;
    public static final String XQUERY_VERSION_1_0_ML = "xquery version \"1.0-ml\";\n";

    public DatabaseContentWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad) {
        this(conf, forestSourceMap, fastLoad, null); 
    }
    
    public DatabaseContentWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad,
        AssignmentManager am) {
        super(conf, forestSourceMap, fastLoad, am);

        if (countBased) {
            metadatas = new URIMetadata[1][batchSize];
        } else {
            metadatas = new URIMetadata[forestIds.length][batchSize];
        }
        isCopyProps = conf.getBoolean(
            ConfigConstants.CONF_COPY_PROPERTIES, true);
        isCopyPerms = conf.getBoolean(
            ConfigConstants.CONF_COPY_PERMISSIONS, true);
        isCopyColls = conf.getBoolean(
            ConfigConstants.CONF_COPY_COLLECTIONS, true);
        isCopyQuality = conf.getBoolean(
            ConfigConstants.CONF_COPY_QUALITY, true);
    }

    /**
     * fetch the options information from conf and metadata, set to the field
     * "options"
     */
    protected ContentCreateOptions newContentCreateOptions(
            DocumentMetadata meta) {
        ContentCreateOptions opt = (ContentCreateOptions)options.clone();
        if (meta != null) {
            if (opt.getQuality() == 0) {
                opt.setQuality(meta.quality);
            }
            HashSet<String> colSet = new HashSet<String>(meta.collectionsList);
            if (opt.getCollections() != null) {
                // union copy_collection and output_collection
                for (String s : opt.getCollections()) {
                    colSet.add(s);
                }
            }
            opt.setCollections(colSet.toArray(new String[colSet.size()]));
            HashSet<ContentPermission> pSet = new HashSet<ContentPermission>(
                    meta.permissionsList);
            if (opt.getPermissions() != null) {
                // union of output_permission & copy_permission
                for (ContentPermission p : opt.getPermissions()) {
                    pSet.add(p);
                }
            }
            opt.setPermissions(
                    pSet.toArray(new ContentPermission[pSet.size()]));
        }       
        return opt;
    }

    @Override
    public void write(DocumentURI key, VALUE value) throws IOException,
        InterruptedException {       
        String uri = InternalUtilities.getUriWithOutputDir(key, outputDir);
        String forestId = ContentOutputFormat.ID_PREFIX;
        int fId = 0;
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
        
        Content content = null;
        DocumentMetadata meta = null;
        if (value instanceof DatabaseDocumentWithMeta) {
            meta = ((DatabaseDocumentWithMeta) value).getMeta();
            ContentCreateOptions opt = newContentCreateOptions(meta);
            MarkLogicDocument doc = (MarkLogicDocument)value;
            opt.setFormat(doc.getContentType().getDocumentFormat());
            if (!meta.isNakedProps()) {
                if (doc.getContentType() == ContentType.BINARY) {
                    content = ContentFactory.newContent(uri,
                            doc.getContentAsByteArray(), opt);
                } else {
                    content = ContentFactory.newContent(uri, 
                            doc.getContentAsText().toString(), opt);
                }
            }
        } else {
            throw new UnsupportedOperationException(value.getClass()
                    + " is not supported.");
        }      
        if(countBased) {
            fId = 0;
        }
        pendingUris[sid].put(content, new DocumentURI(key));
        if (batchSize > 1) {
            if (!meta.isNakedProps()) {
                // add new content
                forestContents[fId][counts[fId]] = content;
                metadatas[fId][counts[fId]++] = new URIMetadata(uri, meta);
            } else {
                // naked properties
                if (isCopyProps) {
                    if (sessions[sid] == null) {
                        sessions[sid] = getSession(forestId);
                    }
                    setDocumentProperties(uri, meta.getProperties(),
                            isCopyPerms?meta.getPermString():null,
                            isCopyColls?meta.getCollectionString():null,
                            isCopyQuality?meta.getQualityString():null, 
                            sessions[sid]);
                    stmtCounts[sid]++;
                }
            }
            if (counts[fId] == batchSize) {
                if (sessions[sid] == null) {
                    sessions[sid] = getSession(forestId);
                }    
                insertBatch(forestContents[fId], sid);     
                stmtCounts[sid]++;
                if (isCopyProps) {
                    // insert properties
                    for (int i = 0; i < counts[fId]; i++) {
                        DocumentMetadata m = metadatas[fId][i].getMeta();
                        String u = metadatas[fId][i].getUri();
                        if (m != null && m.getProperties() != null) {
                            setDocumentProperties(u, m.getProperties(),
                                    null,null,null,sessions[sid]);
                            stmtCounts[sid]++;
                        }
                    }
                }
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
                counts[fId] = 0;
            }
        } else {
            if (sessions[sid] == null) {
                sessions[sid] = getSession(forestId);
            }
            if (content != null) {
                insertContent(content, sid);
                stmtCounts[sid]++;
            }
            //reset forest index for statistical
            if (countBased) {
                sfId = -1;
            }     
            if (isCopyProps && meta.getProperties() != null) {
                boolean naked = content == null;
                setDocumentProperties(uri, meta.getProperties(),
                        isCopyPerms&&naked?meta.getPermString():null,
                        isCopyColls&&naked?meta.getCollectionString():null,
                        isCopyQuality&&naked?meta.getQualityString():null, 
                        sessions[sid]);
                stmtCounts[sid]++;
            }
        }
        if (stmtCounts[sid] == txnSize && needCommit) {
            commit(sid);
            stmtCounts[sid] = 0;
            commitUris[sid].clear();
        }
    }

    protected Session getSession(String forestId, TransactionMode mode) {
        Session session = null;
        ContentSource cs = forestSourceMap.get(forestId);
        if (fastLoad) {
            session = cs.newSession(forestId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connect to forest " + forestId + " on "
                    + session.getConnectionUri().getHost());
            }
        } else {
            session = cs.newSession();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connect to " + session.getConnectionUri().getHost());
            }
        }   
        session.setTransactionMode(mode);
        return session;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
        if (batchSize > 1) {
            int len, sid;
            if (countBased) {
                len = 1;
                sid = sfId;
            } else {
                len = forestIds.length;
                sid = 0;
            }
            for (int i = 0; i < len; i++, sid++) {
                if (counts[i] > 0) {
                    Content[] remainder = new Content[counts[i]];
                    System.arraycopy(forestContents[i], 0, remainder, 0,
                        counts[i]);
                    if (sessions[sid] == null) {
                        String forestId = forestIds[i];
                        sessions[sid] = getSession(forestId);
                    }
                    insertBatch(remainder, sid);
                    stmtCounts[sid]++;
                    if (!isCopyProps) {
                        continue;
                    }
                    for (int j = 0; j < counts[i]; j++) {
                        DocumentMetadata m = metadatas[i][j].getMeta();
                        String u = metadatas[i][j].getUri();
                        if (m != null && m.getProperties() != null) {
                            setDocumentProperties(u, m.getProperties(),
                                isCopyPerms?m.getPermString():null,
                                isCopyColls?m.getCollectionString():null,
                                isCopyQuality?m.getQualityString():null, 
                                sessions[sid]);
                            stmtCounts[sid]++;
                        }
                    }
                }
            }
        }
        for (int i = 0; i < sessions.length; i++) {
            if (sessions[i] != null) {
                if (stmtCounts[i] > 0 && needCommit) {
                    try {
                        sessions[i].commit();
                        succeeded += commitUris[i].size();
                    } catch (RequestException e) {
                        // log error and continue on RequestServerException
                        LOG.warn("Error commiting transaction", e);
                        failed += commitUris[i].size();   
                        for (DocumentURI failedUri : commitUris[i]) {
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Failed document: " + failedUri);
                            }
                        }
                        commitUris[i].clear();
                    } finally {
                        sessions[i].close();
                    }
                } else {
                    sessions[i].close();
                }
            }
        }
        context.getCounter(
                ContentPumpStats.SUCCESSFUL_WRITES).increment(succeeded);
        context.getCounter(
                ContentPumpStats.FAILED_WRITES).increment(failed);
    }

    /**
     * 
     * @param uri
     *            uri of the document whose property is to be set
     * @param xmlString
     *            property in xml string
     * @param forestId
     * @throws RequestException
     */
    protected void setDocumentProperties(String uri, String xmlString,
        String permString, String collString, String quality,
        Session s) {
        String query = XQUERY_VERSION_1_0_ML
            + "declare variable $URI as xs:string external;\n"
            + "declare variable $XML-STRING as xs:string external;\n"
            + "declare variable $PERM-STRING as xs:string external;\n"
            + "declare variable $COLL-STRING as xs:string external;\n"
            + "declare variable $QUALITY-STRING as xs:string external;\n"
            + "xdmp:document-set-properties($URI,\n"
            + "  xdmp:unquote($XML-STRING)/prop:properties/node() )\n"
            + ", if('' eq ($PERM-STRING)) then () else xdmp:document-set-permissions($URI,xdmp:unquote($PERM-STRING)/node())\n"
            + ", if('' eq ($COLL-STRING)) then () else xdmp:document-set-collections($URI,$COLL-STRING)\n"
            + ", if('' eq ($QUALITY-STRING)) then () else xdmp:document-set-quality($URI,xs:integer($QUALITY-STRING))\n"
            ;
        AdhocQuery req = s.newAdhocQuery(query);
        req.setNewStringVariable("URI", uri);
        req.setNewStringVariable("XML-STRING", xmlString);
        req.setNewStringVariable("PERM-STRING", 
                permString==null?"":permString);
        req.setNewStringVariable("COLL-STRING", 
                collString==null||collString.isEmpty()?"":collString);
        req.setNewStringVariable("QUALITY-STRING", 
                quality==null?"":quality);
        try {
            s.submitRequest(req);
        } catch (RequestException ex) {
            LOG.warn("Error setting document properties for " + uri, ex);
        }
    }
}

class URIMetadata {
    String uri;
    DocumentMetadata meta;

    public URIMetadata(String uri, DocumentMetadata meta) {
        super();
        this.uri = uri;
        this.meta = meta;
    }

    public String getUri() {
        return uri;
    }

    public DocumentMetadata getMeta() {
        return meta;
    }

}