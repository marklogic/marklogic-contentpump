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

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.MarkLogicRecordWriter;
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
    private String outputDir;

    /**
     * Content options of the output documents.
     */
    private ContentCreateOptions options;

    /**
     * A map from a forest id to a ContentSource.
     */
    private Map<String, ContentSource> forestSourceMap;

    /**
     * Content lists for each forest.
     */
    private Content[][] forestContents;

    private URIMetadata[][] metadatas;
    /**
     * An array of forest ids
     */
    private String[] forestIds;

    /**
     * Counts of documents per forest.
     */
    private int[] counts;

    /**
     * Whether in fast load mode.
     */
    private boolean fastLoad;

    /**
     * Batch size.
     */
    private int batchSize;

    /**
     * Counts of requests per forest.
     */
    private int[] stmtCounts;

    /**
     * Sessions per forest.
     */
    private Session[] sessions;
    private boolean tolerateErrors;
    
    public static final String XQUERY_VERSION_1_0_ML = "xquery version \"1.0-ml\";\n";

    public DatabaseContentWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad) {
        super(conf, null);

        this.fastLoad = fastLoad;

        this.forestSourceMap = forestSourceMap;

        // arraySize is the number of forests in fast load mode; 1 otherwise.
        int arraySize = forestSourceMap.size();
        forestIds = new String[arraySize];
        forestIds = forestSourceMap.keySet().toArray(forestIds);
        sessions = new Session[arraySize];
        stmtCounts = new int[arraySize];

        outputDir = conf.get(OUTPUT_DIRECTORY);
        batchSize = conf.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        if (batchSize > 1) {
            forestContents = new Content[arraySize][batchSize];
            counts = new int[arraySize];
            metadatas = new URIMetadata[arraySize][batchSize];
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
    private void newContentCreateOptions(DocumentMetadata meta) {
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
        String uri = key.getUri();
        String forestId = DatabaseContentOutputFormat.ID_PREFIX;
        int fId = 0;
        if (fastLoad) {
            // compute forest to write to
            if (outputDir != null && !outputDir.isEmpty()) {
                uri = outputDir.endsWith("/") || uri.startsWith("/") ? outputDir
                    + uri
                    : outputDir + '/' + uri;
            }
            if (uri.endsWith(DocumentMetadata.EXTENSION)) {
                //let the metadata stay in the same forest as its doc
                //because forestContents and metadatas need to be aligned 
                key.setUri(uri.substring(0, uri.length()
                    - DocumentMetadata.EXTENSION.length()));
            } else {
                key.setUri(uri);
            }
            key.validate();
            fId = key.getPlacementId(forestIds.length);

            forestId = forestIds[fId];
        }

        try {
            boolean metaOnly = false;
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
            if (batchSize > 1) {
                if (!meta.isNakedProps()) {
                    // add new content
                    forestContents[fId][counts[fId]] = content;
                    metadatas[fId][counts[fId]++] = new URIMetadata(uri, meta);
                } else {
                    // naked properties
                    if (isCopyProps) {
                        if (sessions[fId] == null) {
                            sessions[fId] = getSession(forestId);
                        }
                        uri = uri.substring(0, uri.length()
                            - DocumentMetadata.NAKED.length());
                        setDocumentProperties(uri, meta.getProperties(),
                            sessions[fId]);
                        stmtCounts[fId]++;
                    }
                }
                if (counts[fId] == batchSize) {
                    if (sessions[fId] == null) {
                        sessions[fId] = getSession(forestId);
                    }
                    
                    List<RequestException> errors = 
                        sessions[fId].insertContentCollectErrors(
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
                    
                    stmtCounts[fId]++;
                    if (isCopyProps) {
                        // insert properties
                        for (int i = 0; i < counts[fId]; i++) {
                            DocumentMetadata m = metadatas[fId][i].getMeta();
                            String u = metadatas[fId][i].getUri();
                            if (m != null && m.getProperties() != null) {
                                setDocumentProperties(u, m.getProperties(),
                                    sessions[fId]);
                                stmtCounts[fId]++;
                            }
                        }
                    }
                    counts[fId] = 0;
                }

            } else {
                if (sessions[fId] == null) {
                    sessions[fId] = getSession(forestId);
                }
                if (content != null) {
                    sessions[fId].insertContent(content);
                    stmtCounts[fId]++;
                }
                // meta's properties is null if CONF_COPY_PROPERTIES is false
                if (meta != null && meta.getProperties() != null) {
                    if (metaOnly) {
                        uri = uri.substring(0, uri.length()
                            - DocumentMetadata.EXTENSION.length());
                    }
                    setDocumentProperties(uri, meta.getProperties(),
                        sessions[fId]);
                    stmtCounts[fId]++;
                }
            }
            if (stmtCounts[fId] >= txnSize
                && sessions[fId].getTransactionMode() == TransactionMode.UPDATE) {
                sessions[fId].commit();
                stmtCounts[fId] = 0;
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
            throw new IOException(e);
        }
    }

    private Session getSession(String forestId) {
        Session session = null;
        ContentSource cs = forestSourceMap.get(forestId);
        if (fastLoad) {
            session = cs.newSession(forestId);

        } else {
            session = cs.newSession();
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
            for (int i = 0; i < forestIds.length; i++) {
                if (counts[i] > 0) {
                    Content[] remainder = new Content[counts[i]];
                    System.arraycopy(forestContents[i], 0, remainder, 0,
                        counts[i]);

                    if (sessions[i] == null) {
                        String forestId = forestIds[i];
                        sessions[i] = getSession(forestId);
                    }

                    try {
                        List<RequestException> errors = 
                            sessions[i].insertContentCollectErrors(remainder);
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
                        stmtCounts[i]++;
                        if (!conf.getBoolean(
                            ConfigConstants.CONF_COPY_PROPERTIES, true)) {
                            continue;
                        }
                        for (int j = 0; j < counts[i]; j++) {
                            DocumentMetadata m = metadatas[i][j].getMeta();
                            String u = metadatas[i][j].getUri();
                            if (m != null && m.getProperties() != null) {
                                setDocumentProperties(u, m.getProperties(),
                                    sessions[i]);
                                stmtCounts[i]++;
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
                        if (sessions[i] != null) {
                            sessions[i].close();
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
                if (stmtCounts[i] > 0 && txnSize > 1) {
                    try {
                        sessions[i].commit();
                    } catch (RequestException e) {
                        LOG.error(e);
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
    private void setDocumentProperties(String _uri, String _xmlString,
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