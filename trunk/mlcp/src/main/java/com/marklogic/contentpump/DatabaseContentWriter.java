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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.MarkLogicRecordWriter;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.AssignmentPolicy;
import com.marklogic.mapreduce.utilities.StatisticalAssignmentPolicy;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RequestPermissionException;
import com.marklogic.xcc.exceptions.RequestServerException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.exceptions.XQueryException;

/**
 * MarkLogicRecordWriter that can 
 * 1) insert content from Archive to MarkLogic Server
 * 2) copy content from source MarkLogic Server to destination MarkLogic Server
 * 
 * @author ali
 * 
 */
public class DatabaseContentWriter<VALUE> extends
    MarkLogicRecordWriter<DocumentURI, VALUE> implements MarkLogicConstants {
    public static final Log LOG = LogFactory
        .getLog(DatabaseContentWriter.class);

    /**
     * Directory of the output documents.
     */
    protected String outputDir;

    /**
     * Content options of the output documents.
     */
    protected ContentCreateOptions options;

    /**
     * A map from a forest id to a ContentSource.
     */
    protected Map<String, ContentSource> forestSourceMap;

    /**
     * Content lists for each forest.
     */
    private Content[][] forestContents;

    private URIMetadata[][] metadatas;
    /**
     * An array of forest ids
     */
    protected String[] forestIds;

    /**
     * Counts of documents per forest.
     */
    private int[] counts;

    /**
     * Whether in fast load mode.
     */
    protected boolean fastLoad;

    /**
     * Batch size.
     */
    private int batchSize;

    /**
     * Counts of requests per forest.
     */
    protected int[] stmtCounts;

    /**
     * Sessions per forest.
     */
    protected Session[] sessions;
    private boolean tolerateErrors;
    
    protected AssignmentManager am;
    protected int sfId;
    //default boolean is false
    protected boolean needFrmtCount;
    
    public static final String XQUERY_VERSION_1_0_ML = "xquery version \"1.0-ml\";\n";

    public DatabaseContentWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad) {
        this(conf, forestSourceMap, fastLoad, null); 
    }
    
    public DatabaseContentWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad,
        AssignmentManager am) {
        super(conf, null);

        this.fastLoad = fastLoad;

        this.forestSourceMap = forestSourceMap;
        this.am = am;
        
        // arraySize is the number of forests in fast load mode; 1 otherwise.
        int arraySize = forestSourceMap.size();
        forestIds = new String[arraySize];
        forestIds = forestSourceMap.keySet().toArray(forestIds);
        sessions = new Session[arraySize];
        stmtCounts = new int[arraySize];

        outputDir = conf.get(OUTPUT_DIRECTORY);
        batchSize = conf.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        if (fastLoad
            && (am.getPolicy().getPolicyKind() == AssignmentPolicy.Kind.STATISTICAL
            || am.getPolicy().getPolicyKind() == AssignmentPolicy.Kind.RANGE)) {
            needFrmtCount = true;
            if (batchSize > 1) {           
                forestContents = new Content[1][batchSize];
                counts = new int[1];
                metadatas = new URIMetadata[1][batchSize];
            }
            sfId = -1;
        } else {
            if (batchSize > 1) {           
                forestContents = new Content[arraySize][batchSize];
                counts = new int[arraySize];
                metadatas = new URIMetadata[arraySize][batchSize];
            }
            sfId = 0;
        }
    }

    private List<ContentPermission> getPermissions(String[] perms) {
        List<ContentPermission> permissions = null;
        if (perms != null && perms.length > 0) {
            int i = 0;
            while (i + 1 < perms.length) {
                String roleName = perms[i++];
                if (roleName == null || roleName.isEmpty()) {
                    LOG.error("Illegal role name: " + roleName);
                    continue;
                }
                String perm = perms[i].trim();
                ContentCapability capability = null;
                if (perm.equalsIgnoreCase(ContentCapability.READ.toString())) {
                    capability = ContentCapability.READ;
                } else if (perm.equalsIgnoreCase(ContentCapability.EXECUTE
                    .toString())) {
                    capability = ContentCapability.EXECUTE;
                } else if (perm.equalsIgnoreCase(ContentCapability.INSERT
                    .toString())) {
                    capability = ContentCapability.INSERT;
                } else if (perm.equalsIgnoreCase(ContentCapability.UPDATE
                    .toString())) {
                    capability = ContentCapability.UPDATE;
                } else {
                    LOG.error("Illegal permission: " + perm);
                }
                if (capability != null) {
                    if (permissions == null) {
                        permissions = new ArrayList<ContentPermission>();
                    }
                    permissions
                        .add(new ContentPermission(capability, roleName));
                }
                i++;
            }
        }
        return permissions;
    }

    /**
     * fetch the options information from conf and metadata, set to the field
     * "options"
     */
    protected void newContentCreateOptions(DocumentMetadata meta) {
        options = new ContentCreateOptions();
        updateCopyOptions(options, meta);
        options.setLanguage(conf.get(OUTPUT_CONTENT_LANGUAGE));
        options.setEncoding(conf.get(OUTPUT_CONTENT_ENCODING,
            DEFAULT_OUTPUT_CONTENT_ENCODING));
        String repairLevel = conf.get(OUTPUT_XML_REPAIR_LEVEL,
            DEFAULT_OUTPUT_XML_REPAIR_LEVEL).toLowerCase();
        if (DocumentRepairLevel.DEFAULT.toString().equals(repairLevel)) {
            options.setRepairLevel(DocumentRepairLevel.DEFAULT);
        } else if (DocumentRepairLevel.NONE.toString().equals(repairLevel)) {
            options.setRepairLevel(DocumentRepairLevel.NONE);
        } else if (DocumentRepairLevel.FULL.toString().equals(repairLevel)) {
            options.setRepairLevel(DocumentRepairLevel.FULL);
        }
    }

    @Override
    public void write(DocumentURI key, VALUE value) throws IOException,
        InterruptedException {
        int fId = 0;
        String uri = getUriWithOutputDir(key, outputDir);
        String forestId = ContentOutputFormat.ID_PREFIX;

        if (fastLoad) {
            if(!needFrmtCount) {
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
            Content content = null;
            DocumentMetadata meta = null;
            if (value instanceof MarkLogicDocumentWithMeta) {
                meta = ((MarkLogicDocumentWithMeta) value).getMeta();
                newContentCreateOptions(meta);
                MarkLogicDocument doc = (MarkLogicDocument) value;
                options.setFormat(doc.getContentType().getDocumentFormat());
                if (!meta.isNakedProps()) {
                    if (doc.getContentType() == ContentType.BINARY) {
                        content = ContentFactory.newContent(uri,
                            doc.getContentAsByteArray(), options);
                    } else {
                        content = ContentFactory.newContent(uri, doc
                            .getContentAsText().toString(), options);
                    }
                }
            } else {
                throw new IOException("unexpected type " + value.getClass());
            }

            boolean isCopyProps = conf.getBoolean(
                ConfigConstants.CONF_COPY_PROPERTIES, true);
            if(needFrmtCount) {
                fId = 0;
            }
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
                            sessions[sid]);
                        stmtCounts[sid]++;
                    }
                }
                if (counts[fId] == batchSize) {
                    if (sessions[sid] == null) {
                        sessions[sid] = getSession(forestId);
                    }
                    
                    List<RequestException> errors = 
                        sessions[sid].insertContentCollectErrors(
                                forestContents[fId]);
                    if (errors != null) {
                        for (RequestException ex : errors) {
                            if (ex instanceof XQueryException) {
                                LOG.warn(((XQueryException) ex).getFormatString());
                            } else {
                                LOG.warn(ex.getMessage());
                            }
                        }                       
                    }
                    
                    stmtCounts[sid]++;
                    if (isCopyProps) {
                        // insert properties
                        for (int i = 0; i < counts[fId]; i++) {
                            DocumentMetadata m = metadatas[fId][i].getMeta();
                            String u = metadatas[fId][i].getUri();
                            if (m != null && m.getProperties() != null) {
                                setDocumentProperties(u, m.getProperties(),
                                    sessions[sid]);
                                stmtCounts[sid]++;
                            }
                        }
                    }
                    
                    //reset forest index for statistical
                    if (needFrmtCount) {
                        sfId = -1;
                    }
                    counts[fId] = 0;
                }

            } else {
                if (sessions[sid] == null) {
                    sessions[sid] = getSession(forestId);
                }
                if (content != null) {
                    sessions[sid].insertContent(content);
                    stmtCounts[sid]++;
                }
                //reset forest index for statistical
                if (needFrmtCount) {
                    sfId = -1;
                }
                
                if (isCopyProps && meta.getProperties() != null) {
                    setDocumentProperties(uri, meta.getProperties(),
                        sessions[sid]);
                    stmtCounts[sid]++;
                }
            }
            if (stmtCounts[sid] >= txnSize
                && sessions[sid].getTransactionMode() == TransactionMode.UPDATE) {
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
            if (needFrmtCount) {
                rollbackDocCount(sid);
            }
            throw new IOException(e);
        }
    }

    private Session getSession(String forestId) {
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

        if (txnSize > 1 || (batchSize > 1 && tolerateErrors)) {
            session.setTransactionMode(TransactionMode.UPDATE);
        }
        return session;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
        if (batchSize > 1) {
            int len, sid;
            if (needFrmtCount) {
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

                    try {
                        List<RequestException> errors = 
                            sessions[sid].insertContentCollectErrors(remainder);
                        if (errors != null) {
                            for (RequestException ex : errors) {
                                if (ex instanceof XQueryException) {
                                    LOG.warn(((XQueryException) ex).
                                            getFormatString());
                                } else {
                                    LOG.warn(ex.getMessage());
                                }
                            }                       
                        }
                        //RequestException if any is thrown before docCount is updated
                        //so docCount doesn't need to rollback in this try-catch
                        if (needFrmtCount) {
                            stmtCounts[sfId]++;
                            sfId = -1;
                        } else {
                            stmtCounts[i]++;
                        }
                        if (!conf.getBoolean(
                            ConfigConstants.CONF_COPY_PROPERTIES, true)) {
                            continue;
                        }
                        for (int j = 0; j < counts[i]; j++) {
                            DocumentMetadata m = metadatas[i][j].getMeta();
                            String u = metadatas[i][j].getUri();
                            if (m != null && m.getProperties() != null) {
                                setDocumentProperties(u, m.getProperties(),
                                    sessions[sid]);
                                stmtCounts[sid]++;
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
                        LOG.error(e);
                        if (sessions[sid] != null) {
                            sessions[sid].close();
                        }
                        if (needFrmtCount) {
                            rollbackDocCount(sid);
                        }
                        if (e instanceof ServerConnectionException
                            || e instanceof RequestPermissionException) {
                            throw new IOException(e);
                        }
                    }
                }
            }
        }
        for (int i = 0; i < sessions.length; i++) {
            if (sessions[i] != null) {
                if (stmtCounts[i] > 0
                    && sessions[i].getTransactionMode() == TransactionMode.UPDATE) {
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
    }

    @Override
    public int getTransactionSize(Configuration conf) {
        // return the specified txn size
        if (conf.get(TXN_SIZE) != null) {
            int txnSize = conf.getInt(TXN_SIZE, 0);
            return txnSize <= 0 ? 1 : txnSize;
        } 
        return 1000 / conf.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }

    /**
     * 
     * @param _uri
     *            uri of the document whose property is to be set
     * @param _xmlString
     *            property in xml string
     * @param forestId
     * @throws RequestException
     */
    protected void setDocumentProperties(String _uri, String _xmlString,
        Session s) throws RequestException {
        String query = XQUERY_VERSION_1_0_ML
            + "declare variable $URI as xs:string external;\n"
            + "declare variable $XML-STRING as xs:string external;\n"
            + "xdmp:document-set-properties($URI,\n"
            + "  xdmp:unquote($XML-STRING)/prop:properties/node() )\n";
        AdhocQuery req = s.newAdhocQuery(query);
        req.setNewStringVariable("URI", _uri);
        req.setNewStringVariable("XML-STRING", _xmlString);
        s.submitRequest(req);
    }

    private void updateCopyOptions(ContentCreateOptions options,
        DocumentMetadata meta) {
        String quality = conf.get(OUTPUT_QUALITY);
        if (quality != null) {
            // use output_quality
            options.setQuality(Integer.parseInt(quality));
        } else if (meta != null) {
            // use the quality stored in meta
            options.setQuality(meta.quality);
        } // else use default, do nothing

        String[] collections = conf.getStrings(OUTPUT_COLLECTION);
        String[] perms = conf.getStrings(OUTPUT_PERMISSION);
        List<ContentPermission> permissions = getPermissions(perms);
        if (meta != null) {
            HashSet<String> colSet = new HashSet<String>(meta.collectionsList);
            HashSet<ContentPermission> pSet = new HashSet<ContentPermission>(
                meta.permissionsList);
            if (collections != null) {
                // union copy_collection and output_collection
                for (String s : collections) {
                    colSet.add(s);
                }
            }
            if (permissions != null) {
                // union of output_permission & copy_permission
                for (ContentPermission p : permissions) {
                    pSet.add(p);
                }
            }
            options.setCollections(colSet.toArray(new String[colSet.size()]));
            options.setPermissions(pSet.toArray(new ContentPermission[pSet
                .size()]));
        } else {
            // empty metadata
            if (collections != null) {
                options.setCollections(collections);
            }
            if (permissions != null) {
                options.setPermissions(permissions
                    .toArray(new ContentPermission[permissions.size()]));
            }
        }
    }

    protected void rollbackDocCount(int fId) {
        StatisticalAssignmentPolicy sap = (StatisticalAssignmentPolicy) am
            .getPolicy();
        sap.updateStats(fId, -batchSize);
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