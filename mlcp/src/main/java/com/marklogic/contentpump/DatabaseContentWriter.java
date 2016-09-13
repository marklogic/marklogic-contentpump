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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.mapreduce.MarkLogicCounter;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.ContentWriter;
import com.marklogic.mapreduce.DocumentURI;
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
import com.marklogic.xcc.types.ValueType;

/**
 * MarkLogicRecordWriter that can 
 * 1) insert content from Archive to MarkLogic Server
 * 2) copy content from source MarkLogic Server to destination MarkLogic Server
 * 
 * @author ali
 * 
 */
public class DatabaseContentWriter<VALUE> extends
    ContentWriter<VALUE> implements ConfigConstants {
    public static final Log LOG = 
            LogFactory.getLog(DatabaseContentWriter.class);

    private URIMetadata[][] metadatas;
    
    protected boolean isCopyProps;
    protected boolean isCopyPerms;
    
    public static final String XQUERY_VERSION_1_0_ML = 
            "xquery version \"1.0-ml\";\n";

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
        isCopyProps = conf.getBoolean(CONF_COPY_PROPERTIES, true);
        isCopyPerms = conf.getBoolean(CONF_COPY_PERMISSIONS, true);
    }

    /**
     * fetch the options information from conf and metadata, set to the field
     * "options"
     */
    protected ContentCreateOptions newContentCreateOptions(
            DocumentMetadata meta) {
        ContentCreateOptions opt = (ContentCreateOptions)options.clone();
        if (meta != null) {
            if (isCopyQuality && opt.getQuality() == 0) {
                opt.setQuality(meta.quality);
            }
            if (isCopyColls) {
                if (opt.getCollections() != null) {
                    HashSet<String> colSet = 
                            new HashSet<String>(meta.collectionsList);
                    // union copy_collection and output_collection
                    for (String s : opt.getCollections()) {
                        colSet.add(s);
                    }
                    opt.setCollections(
                            colSet.toArray(new String[colSet.size()]));
                } else {
                    opt.setCollections(meta.getCollections());
                }
            }      
            if (isCopyPerms) {
                if (opt.getPermissions() != null) {
                    HashSet<ContentPermission> pSet = 
                         new HashSet<ContentPermission>(meta.permissionsList);
                    // union of output_permission & copy_permission
                    for (ContentPermission p : opt.getPermissions()) {
                        pSet.add(p);
                    }
                    opt.setPermissions(
                            pSet.toArray(new ContentPermission[pSet.size()]));
                } else {
                    opt.setPermissions(meta.getPermissions());
                }
            }
            if (isCopyMeta) {
                opt.setMetadata(meta.meta);
            }
        }       
        return opt;
    }

    @Override
    public void write(DocumentURI key, VALUE value) throws IOException,
        InterruptedException {       
        String uri = InternalUtilities.getUriWithOutputDir(key, outputDir);
        String csKey;
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
            csKey = forestIds[fId];
        } else {
            csKey = forestIds[hostId];
        }
        int sid = fId;
        
        Content content = null;
        DocumentMetadata meta = null;
        if (value instanceof DatabaseDocumentWithMeta) {
            meta = ((DatabaseDocumentWithMeta) value).getMeta();
            ContentCreateOptions opt = newContentCreateOptions(meta);
            MarkLogicDocument doc = (MarkLogicDocument)value;
            if (!meta.isNakedProps()) {
                opt.setFormat(doc.getContentType().getDocumentFormat());
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
        boolean inserted = false;
        if (batchSize > 1) {
            if (!meta.isNakedProps()) {
                // add new content
                forestContents[fId][counts[fId]] = content;
                metadatas[fId][counts[fId]++] = new URIMetadata(uri, meta);
            } else if (isCopyProps) { // naked properties
                if (sessions[sid] == null) {
                    sessions[sid] = getSession(csKey);
                }
                setDocumentProperties(uri, meta.getProperties(),
                        isCopyPerms?meta.getPermString():null,
                        isCopyColls?meta.getCollectionString():null,
                        isCopyQuality?meta.getQualityString():null, 
                        isCopyMeta?meta.getMeta():null, sessions[sid]);
                stmtCounts[sid]++;
            }
            if (counts[fId] == batchSize) {
                if (sessions[sid] == null) {
                    sessions[sid] = getSession(csKey);
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
                                    null,null,null,null,sessions[sid]);
                            stmtCounts[sid]++;
                        }
                    }
                }
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
                counts[fId] = 0;
                inserted = true;
            }
        } else { // batchSize <= 1
            if (sessions[sid] == null) {
                sessions[sid] = getSession(csKey);
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
                        isCopyMeta&&naked?meta.meta:null, sessions[sid]);
                stmtCounts[sid]++;
            }
            inserted = true;
        }
        boolean committed = false;
        if (stmtCounts[sid] == txnSize && needCommit) {
            commit(sid);
            stmtCounts[sid] = 0;
            commitUris[sid].clear();
            committed = true;
        }
        if ((!fastLoad) && ((inserted && (!needCommit)) || committed)) { 
            // rotate to next host and reset session
            hostId = (hostId + 1)%forestIds.length;
            sessions[0] = null;
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
                len = fastLoad ? forestIds.length : 1;
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
                                null, null, null, null, sessions[sid]);
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
                        LOG.error("Error commiting transaction", e);
                        failed += commitUris[i].size();   
                        for (DocumentURI failedUri : commitUris[i]) {
                            LOG.warn("Failed document " + failedUri);
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
            MarkLogicCounter.OUTPUT_RECORDS_COMMITTED).increment(succeeded);
        context.getCounter(
            MarkLogicCounter.OUTPUT_RECORDS_FAILED).increment(failed);
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
        Map<String, String> meta, Session s) {
        String query = XQUERY_VERSION_1_0_ML
            + "declare variable $URI as xs:string external;\n"
            + "declare variable $XML-STRING as xs:string external;\n"
            + "declare variable $PERM-STRING as xs:string external;\n"
            + "declare variable $COLL-STRING as xs:string external;\n"
            + "declare variable $QUALITY-STRING as xs:string external;\n"
            + (meta == null ? 
                    "" : "declare variable $META as map:map external;\n")
            + "xdmp:document-set-properties($URI,\n"
            + "  xdmp:unquote($XML-STRING)/prop:properties/node() )\n"
            + ", if('' eq ($PERM-STRING)) then () else \n"
            + "xdmp:document-set-permissions($URI, \n"
            + "xdmp:unquote($PERM-STRING)/node()/sec:permission)\n"
            + ", if('' eq ($COLL-STRING)) then () else \n"
            + "let $f := fn:function-lookup(xs:QName('xdmp:from-json-string'), 1)\n"
            + "return if (fn:exists($f)) then \n"
            + "xdmp:document-set-collections($URI,json:array-values($f($COLL-STRING)))\n"
            + "else xdmp:document-set-collections($URI,json:array-values(xdmp:from-json($COLL-STRING)))\n"
            + ", if('' eq ($QUALITY-STRING)) then () else xdmp:document-set-quality($URI,xs:integer($QUALITY-STRING))\n"
            + (meta == null ?
                    "" : ", (let $f := fn:function-lookup(xs:QName('xdmp:document-set-metadata'),1)\n"
                    + "return if (exists($f)) then $f($URI,$META) else ())\n");
        if (LOG.isDebugEnabled()) {
            LOG.debug(query);
        }
        AdhocQuery req = s.newAdhocQuery(query);
        req.setNewStringVariable("URI", uri);
        req.setNewStringVariable("XML-STRING", xmlString);
        req.setNewStringVariable("PERM-STRING", 
                permString==null?"":permString);
        req.setNewStringVariable("COLL-STRING", 
                collString==null||collString.isEmpty()?"":collString);
        req.setNewStringVariable("QUALITY-STRING", 
                quality==null?"":quality);
        if (meta != null) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            for (Map.Entry<String, String> entry : meta.entrySet()) {
                node.put(entry.getKey(), entry.getValue());
            }
            req.setNewVariable("META", ValueType.JS_OBJECT, node);
        }
        
        try {
            s.submitRequest(req);
        } catch (RequestException ex) {
            LOG.error("Error setting document properties for " + uri, ex);
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
