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
import java.util.HashSet;
import java.util.Map;
import java.util.Vector;

import com.marklogic.xcc.exceptions.QueryException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicCounter;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.ContentWriter;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.ValueFactory;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XName;
import com.marklogic.xcc.types.XdmValue;
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
    protected XdmValue[][] propertyUris = null;
    protected XdmValue[][] propertyXmlStrings = null;
    protected int[] propertyCounts = null;

    protected boolean isCopyProps;
    protected boolean isCopyPerms;
    
    public static final String XQUERY_VERSION_1_0_ML = 
            "xquery version \"1.0-ml\";\n";

    public DatabaseContentWriter(Configuration conf,
        Map<String, ContentSource> hostSourceMap, boolean fastLoad) {
        this(conf, hostSourceMap, fastLoad, null); 
    }
    
    public DatabaseContentWriter(Configuration conf,
            Map<String, ContentSource> hostSourceMap, boolean fastLoad,
            AssignmentManager am) {
        super(conf, hostSourceMap, fastLoad, am);
        
        int arraySize = countBased ? 1 : forestIds.length;
        metadatas = new URIMetadata[arraySize][batchSize];
        isCopyProps = conf.getBoolean(CONF_COPY_PROPERTIES, true);
        isCopyPerms = conf.getBoolean(CONF_COPY_PERMISSIONS, true);

        if (effectiveVersion >= BATCH_MIN_VERSION && isCopyProps) {
            propertyUris = new XdmValue[arraySize][batchSize];
            propertyXmlStrings = new XdmValue[arraySize][batchSize];
            propertyCounts = new int[arraySize];
        }
    }

    /**
     * fetch the options information from conf and metadata, set to the field
     * "options"
     */
    protected static ContentCreateOptions newContentCreateOptions(
            DocumentMetadata meta, ContentCreateOptions options, 
            boolean isCopyColls, boolean isCopyQuality, boolean isCopyMeta,
            boolean isCopyPerms, long effectiveVersion) {
        ContentCreateOptions opt = (ContentCreateOptions)options.clone();
        if (meta != null) {
            if (isCopyQuality && opt.getQuality() == 0) {
                opt.setQuality(meta.quality);
            }
            if (isCopyColls) {
                if (opt.getCollections() != null) {
                    HashSet<String> colSet =
                        new HashSet<>(meta.collectionsList);
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
                if (effectiveVersion < MarkLogicConstants.MIN_NODEUPDATE_VERSION &&
                        meta.isNakedProps()) {
                    boolean reset = false;
                    Vector<ContentPermission> perms = new Vector<>();
                    for (ContentPermission perm : meta.permissionsList) {
                        if (!perm.getCapability().toString().equals(
                                ContentPermission.NODE_UPDATE.toString())) {
                            perms.add(perm);
                        } else {
                            reset = true;
                        }
                    }
                    if (reset) {
                        meta.clearPermissions();
                        meta.addPermissions(perms);
                        meta.setPermString(null);
                    }
                }
                if (opt.getPermissions() != null) {
                    HashSet<ContentPermission> pSet =
                        new HashSet<>(meta.permissionsList);
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
        } 
        int sid = fId;
        
        Content content = null;
        DocumentMetadata meta = null;
        if (value instanceof DatabaseDocumentWithMeta) {
            try {
                meta = ((DatabaseDocumentWithMeta) value).getMeta();
                ContentCreateOptions opt = newContentCreateOptions(meta, options,
                    isCopyColls, isCopyQuality, isCopyMeta, isCopyPerms,
                    effectiveVersion);
                MarkLogicDocument doc = (MarkLogicDocument)value;
                if (meta == null || !meta.isNakedProps()) {
                    opt.setFormat(doc.getContentType().getDocumentFormat());
                    if (doc.getContentType() == ContentType.BINARY) {
                        content = ContentFactory.newContent(uri,
                                doc.getContentAsByteArray(), opt);
                    } else {
                        content = ContentFactory.newContent(uri,
                                doc.getContentAsText().toString(), opt);
                    }
                }
            } catch (Exception e) {
                failed++;
                LOG.error(getFormattedBatchId() + "Document failed permanently: "
                    + uri);
                return;
            }
        } else {
            throw new UnsupportedOperationException(value.getClass()
                    + " is not supported.");
        }      
        if(countBased) {
            fId = 0;
        }
        pendingUris[sid].put(content, new DocumentURI(key));
        if (meta == null || !meta.isNakedProps()) {
            // add new content
            forestContents[fId][counts[fId]] = content;
            // add properties
            if (propertyUris != null && meta != null && 
                    meta.getProperties() != null) {
                propertyUris[fId][propertyCounts[fId]] = 
                        ValueFactory.newValue(ValueType.XS_STRING, uri);
                propertyXmlStrings[fId][propertyCounts[fId]] = 
                        ValueFactory.newValue(ValueType.XS_STRING, 
                                meta.getProperties());
                propertyCounts[fId]++;
                counts[fId]++;
            } else {
                metadatas[fId][counts[fId]++] = new URIMetadata(uri, meta);
            }
        } else if (isCopyProps) { // naked properties
            if (sessions[sid] == null) {
                sessions[sid] = getSession(sid, false);
            }
            boolean suc = setDocumentProperties(uri, meta.getProperties(),
                    isCopyPerms?meta.getPermString():null,
                    isCopyColls?meta.getCollectionString():null,
                    isCopyQuality?meta.getQualityString():null, 
                    isCopyMeta?meta.getMeta():null, sessions[sid]);
            stmtCounts[sid]++;
            if (suc && needCommit) {
                commitUris[sid].add(key);
            } else {
                failed++;
            }
        }

        boolean inserted = false;
        boolean committed = false;
        if (counts[fId] == batchSize) {
            commitRetry = 0;
            commitSleepTime = MIN_SLEEP_TIME;
            if (sessions[sid] == null) {
                sessions[sid] = getSession(sid, false);
            }
            while (commitRetry < commitRetryLimit) {
                inserted = false;
                committed = false;
                if (commitRetry > 0) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getFormattedBatchId() +
                            "Retrying committing batch, attempts: " +
                            commitRetry + "/" + MAX_RETRIES);
                    }
                }
                try {
                    insertBatch(forestContents[fId], sid);
                } catch (Exception e) {}
                stmtCounts[sid]++;
                if (isCopyProps) { //prop batch insert
                    if (propertyUris != null) {
                        setBatchProperties(fId, sessions[sid]);
                        stmtCounts[sid]++;
                        propertyCounts[fId] = 0;
                    } else {
                        for (int i = 0; i < counts[fId]; i++) {
                            DocumentMetadata m = metadatas[fId][i].getMeta();
                            String u = metadatas[fId][i].getUri();
                            if (m != null && m.getProperties() != null) {
                                boolean suc = setDocumentProperties(u,
                                    m.getProperties(), null, null, null, null,
                                    sessions[sid]);
                                stmtCounts[sid]++;
                                if (suc && needCommit) {
                                    commitUris[sid].add(key);
                                }
                            }
                        }
                    }
                }
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
                counts[fId] = 0;
                inserted = true;

                if (needCommit && stmtCounts[sid] >= txnSize) {
                    try {
                        commit(sid);
                        if (commitRetry > 0) {
                            LOG.info(getFormattedBatchId() +
                                "Retrying committing batch is successful");
                        }
                    } catch (XccConfigException e) {
                        LOG.error("XccConfigException:" + e);
                        throw new IOException(e);
                    } catch (QueryException e) {
                        LOG.error("QueryException:" + e);
                        throw new IOException(e);
                    } catch (Exception e) {
                        LOG.warn(getFormattedBatchId() +
                            "Failed committing transaction: " + e.getMessage());
                        if (e instanceof RequestException){
                            if (!((RequestException)e).isRetryable())
                                throw new IOException(e);
                        }
                        if (needCommitRetry() && ++commitRetry < commitRetryLimit) {
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
                            LOG.error(getFormattedBatchId() + "Document failed permanently: " + failedUri);
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
            pendingUris[sid].clear();
        }

        if (stmtCounts[sid] >= txnSize && needCommit) { // For naked properties
            try {
                commit(sid);
            } catch (Exception e) {
                LOG.warn(getFormattedBatchId() +
                    "Failed committing transaction: " + e.getMessage());
                handleCommitExceptions(sid);
            } finally {
                stmtCounts[sid] = 0;
                committed = true;
            }
        }

        if ((!fastLoad) && ((inserted && (!needCommit)) || committed)) {
            // rotate to next host and reset session
            hostId = (hostId + 1)%forestIds.length;
            sessions[0] = null;
        }
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
                if (pendingUris[sid].size() > 0) {
                    commitRetry = 0;
                    commitSleepTime = MIN_SLEEP_TIME;
                    Content[] remainder = new Content[counts[i]];
                    System.arraycopy(forestContents[i], 0, remainder, 0,
                            counts[i]);
                    if (sessions[sid] == null) {
                        sessions[sid] = getSession(i, false);
                    }
                    while (commitRetry < commitRetryLimit) {
                        if (commitRetry > 0) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(getFormattedBatchId() +
                                    "Retrying committing batch, attempts: " +
                                    commitRetry + "/" + MAX_RETRIES);
                            }
                        }
                        try {
                            insertBatch(remainder, sid);
                        } catch (Exception e) {}
                        stmtCounts[sid]++;
                        if (!isCopyProps) {
                            break;
                        }
                        if (propertyUris != null && propertyCounts[i] > 0) {
                            setBatchProperties(i, sessions[sid]);
                            stmtCounts[sid]++;
                        } else if (propertyUris == null) {
                            // non-batch insert props
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

                        if (needCommit && stmtCounts[sid] > 0) {
                            try {
                                commit(sid);
                                if (commitRetry > 0) {
                                    LOG.info(getFormattedBatchId() +
                                        "Retrying committing batch is successful");
                                }
                            } catch (Exception e) {
                                LOG.warn(getFormattedBatchId() +
                                    "Failed committing transaction: " + e.getMessage());
                                if (needCommitRetry() && ++commitRetry < commitRetryLimit) {
                                    LOG.warn(getFormattedBatchId() + "Failed during committing");
                                    handleCommitExceptions(sid);
                                    commitSleepTime = sleep(commitSleepTime);
                                    sessions[sid] = getSession(sid, true);
                                    stmtCounts[sid] = 0;
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
                                sessions[sid].close();
                            }
                        }
                        break;
                    }
                    batchId++;
                    pendingUris[sid].clear();
                }
            }
        }

        closeSessions();

        context.getCounter(
                MarkLogicCounter.OUTPUT_RECORDS_COMMITTED).increment(succeeded);
        context.getCounter(
                MarkLogicCounter.OUTPUT_RECORDS_FAILED).increment(failed);
    }

    protected boolean setBatchProperties(int i, Session s) {
        if (propertyCounts[i] == 0) return true;
        
        String query = XQUERY_VERSION_1_0_ML
                + "declare variable $URI as xs:string* external;\n" +
                "declare variable $XML-STRING as xs:string* external;\n" +
                "for $docuri in $URI\n" +
                "return(       \n" +
                "xdmp:document-set-properties($docuri, xdmp:unquote(fn:head($XML-STRING))/prop:properties/node() ),\n" +
                "    xdmp:set($XML-STRING, fn:tail($XML-STRING))\n" +
                "    ); ";
        if (LOG.isDebugEnabled()) {
            LOG.debug(query);
        }
        AdhocQuery req = s.newAdhocQuery(query);
        
        XdmValue[] uriArray = propertyUris[i];
        XdmValue[] xmlStringArray = propertyXmlStrings[i]; 
        if (propertyCounts[i] < batchSize) {
            uriArray = new XdmValue[propertyCounts[i]];
            xmlStringArray = new XdmValue[propertyCounts[i]];
            System.arraycopy(propertyUris[i], 0, 
                    uriArray, 0, propertyCounts[i]);
            System.arraycopy(propertyXmlStrings[i], 0, 
                    xmlStringArray, 0, propertyCounts[i]);
        }

        req.setNewVariables(new XName("URI"), uriArray);
        req.setNewVariables(new XName("XML-STRING"), xmlStringArray);

        try {
            s.submitRequest(req);
            return true;
        } catch (RequestException ex) {
            for (XdmValue failedUri : uriArray) {
                LOG.error("Error batch setting document properties for: " + 
                        failedUri.asString() );
            }
            LOG.error( ex.getMessage());
            return false;
        }
    }

    /**
     *
     * @param uri uri of the document whose property is to be set
     * @param xmlString property in xml string
     * @param permString
     * @param collString
     * @param quality
     * @param meta
     * @param s
     * @return
     */
    protected static boolean setDocumentProperties(String uri,
                                                   String xmlString, String permString, String collString, String quality,
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
                "" : ", (let $f := fn:function-lookup(xs:QName('xdmp:document-set-metadata'),2)\n"
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
            return true;
        } catch (RequestException ex) {
            LOG.error("Error setting document properties for " + uri + ": " +
                    ex.getMessage());
            return false;
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
