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
package com.marklogic.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.AssignmentPolicy;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.StatisticalAssignmentPolicy;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;
import com.marklogic.xcc.exceptions.ContentInsertException;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RequestServerException;
import com.marklogic.xcc.exceptions.XQueryException;

/**
 * MarkLogicRecordWriter that inserts content to MarkLogicServer.
 * 
 * @author jchen
 *
 */
public class ContentWriter<VALUEOUT> 
extends MarkLogicRecordWriter<DocumentURI, VALUEOUT> implements MarkLogicConstants {
    public static final Log LOG = LogFactory.getLog(ContentWriter.class);
    
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
    protected Content[][] forestContents;
    
    /**
     * An array of forest ids
     */
    protected String[] forestIds;
    
    /** 
     * Counts of documents per forest.
     */
    protected int[] counts;
    
    /**
     * URIs of the documents in a batch.
     */
    protected HashMap<Content, DocumentURI>[] pendingUris;
    
    /**
     * URIs of the documents to be committed.
     */
    protected List<DocumentURI>[] commitUris;
    
    /**
     * Whether in fast load mode.
     */
    protected boolean fastLoad;
    
    /**
     * Batch size.
     */
    protected int batchSize;
    
    /**
     * Counts of requests per forest.
     */
    protected int[] stmtCounts;
    
    /**
     * Sessions per forest.
     */
    protected Session[] sessions;
    
    private boolean formatNeeded;
    
    private FileSystem fs;
    
    protected InputStream is;
    
    private boolean streaming;
    
    private boolean tolerateErrors;

    protected AssignmentManager am;
    
    //fIdx cached for statistical policy
    protected int sfId;
    
    protected boolean countBased;
    
    /** role-id -> role-name mapping **/
    protected LinkedMapWritable roleMap;
    
    protected HashMap<String,ContentPermission[]> permsMap;
    
    protected int succeeded = 0;
    
    protected int failed = 0;
    
    protected boolean needCommit = false;
    
    public ContentWriter(Configuration conf, 
        Map<String, ContentSource> forestSourceMap, boolean fastLoad) {
        this(conf, forestSourceMap, fastLoad, null);
    }
    
    public ContentWriter(Configuration conf,
        Map<String, ContentSource> forestSourceMap, boolean fastLoad,
        AssignmentManager am) {
        super(conf, null);
        
        this.fastLoad = fastLoad;
        
        this.forestSourceMap = forestSourceMap;
        
        this.am = am;
        
        permsMap = new HashMap<String,ContentPermission[]>();
        
        // arraySize is the number of forests in fast load mode; 1 otherwise.
        int arraySize = forestSourceMap.size();
        forestIds = new String[arraySize];
        // key order in key set is guaranteed by LinkedHashMap,
        // i.e., the order keys are inserted
        forestIds = forestSourceMap.keySet().toArray(forestIds);
        sessions = new Session[arraySize];
        stmtCounts = new int[arraySize];
        
        outputDir = conf.get(OUTPUT_DIRECTORY);
        batchSize = conf.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        
        pendingUris = new HashMap[arraySize];
        for (int i = 0; i < arraySize; i++) {
            pendingUris[i] = new HashMap<Content, DocumentURI>();
        }

        if (fastLoad
            && (am.getPolicy().getPolicyKind() == AssignmentPolicy.Kind.STATISTICAL
            || am.getPolicy().getPolicyKind() == AssignmentPolicy.Kind.RANGE)) {
            countBased = true;
            if (batchSize > 1) {           
                forestContents = new Content[1][batchSize];
                counts = new int[1];
            }
            sfId = -1;
        } else {
            if (batchSize > 1) {           
                forestContents = new Content[arraySize][batchSize];
                counts = new int[arraySize];                
            }
            sfId = 0;
        }
        
        String[] perms = conf.getStrings(OUTPUT_PERMISSION);
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
                } else if (perm.equalsIgnoreCase(ContentCapability.EXECUTE.toString())) {
                    capability = ContentCapability.EXECUTE;
                } else if (perm.equalsIgnoreCase(ContentCapability.INSERT.toString())) {
                    capability = ContentCapability.INSERT;
                } else if (perm.equalsIgnoreCase(ContentCapability.UPDATE.toString())) {
                    capability = ContentCapability.UPDATE;
                } else {
                    LOG.error("Illegal permission: " + perm);
                }
                if (capability != null) {
                    if (permissions == null) {
                        permissions = new ArrayList<ContentPermission>();
                    }
                    permissions.add(new ContentPermission(capability, roleName));
                }
                i++;
            }
        }
        
        options = new ContentCreateOptions();
        String[] collections = conf.getStrings(OUTPUT_COLLECTION);
        if (collections != null) {
            for (int i = 0; i < collections.length; i++) {
                collections[i] = collections[i].trim();
            }
            options.setCollections(collections);
        }
        
        options.setQuality(conf.getInt(OUTPUT_QUALITY, 0));
        if (permissions != null) {
            options.setPermissions(permissions.toArray(
                    new ContentPermission[permissions.size()]));
        } 
        String contentTypeStr = conf.get(CONTENT_TYPE, DEFAULT_CONTENT_TYPE);
        ContentType contentType = ContentType.valueOf(contentTypeStr);
        if (contentType == ContentType.UNKNOWN) {
            formatNeeded = true;
        } else {
            options.setFormat(contentType.getDocumentFormat());
        }
        
        options.setLanguage(conf.get(OUTPUT_CONTENT_LANGUAGE));
        String repairLevel = conf.get(OUTPUT_XML_REPAIR_LEVEL,
                DEFAULT_OUTPUT_XML_REPAIR_LEVEL).toLowerCase();
        options.setNamespace(conf.get(OUTPUT_CONTENT_NAMESPACE));
        if (DocumentRepairLevel.DEFAULT.toString().equals(repairLevel)){
            options.setRepairLevel(DocumentRepairLevel.DEFAULT);
        }
        else if (DocumentRepairLevel.NONE.toString().equals(repairLevel)){
            options.setRepairLevel(DocumentRepairLevel.NONE);
        }
        else if (DocumentRepairLevel.FULL.toString().equals(repairLevel)){
            options.setRepairLevel(DocumentRepairLevel.FULL);
        }
        
        streaming = conf.getBoolean(OUTPUT_STREAMING, false);
        tolerateErrors = conf.getBoolean(OUTPUT_TOLERATE_ERRORS, false);
        
        String encoding = conf.get(MarkLogicConstants.OUTPUT_CONTENT_ENCODING);
        if (encoding != null) {
            options.setEncoding(encoding);
        }
        
        options.setTemporalCollection(conf.get(TEMPORAL_COLLECTION));
        
        needCommit = txnSize > 1 || (batchSize > 1 && tolerateErrors);
        if (needCommit) {
            commitUris = new ArrayList[arraySize];
            for (int i = 0; i < arraySize; i++) {
                commitUris[i] = new ArrayList<DocumentURI>(txnSize*batchSize);
            }
        }
    }

    protected Content createContent(DocumentURI key, VALUEOUT value) 
            throws IOException {
        String uri = key.getUri();
        Content content = null;
        if (value instanceof Text) {
            if (formatNeeded) {
                options.setFormat(DocumentFormat.TEXT);
                formatNeeded = false;
            }
            options.setEncoding(DEFAULT_OUTPUT_CONTENT_ENCODING);
            content = ContentFactory.newContent(uri, 
                    ((Text) value).getBytes(), 0, 
                    ((Text)value).getLength(), options);
        } else if (value instanceof MarkLogicNode) {
            if (formatNeeded) {
                options.setFormat(DocumentFormat.XML);
                formatNeeded = false;
            }
            content = ContentFactory.newContent(uri, 
                    ((MarkLogicNode)value).get(), options);
        } else if (value instanceof DOMDocument) {
            content = ContentFactory.newContent(uri, 
                    ((DOMDocument) value).getContentAsMarkLogicNode().get(), 
                    options);
        } else if (value instanceof JSONDocument) {
            JSONDocument doc = (JSONDocument)value;
            content = ContentFactory.newContent(uri, 
                    doc.getContentAsByteArray(), options);
        } else if (value instanceof BinaryDocument) {
            BinaryDocument doc = (BinaryDocument)value;
            if (doc.isStreamable()) {
                InputStream is = null;
                try {
                    is = doc.getContentAsByteStream();
                    content = ContentFactory.newUnBufferedContent(uri, is, 
                            options);
                } catch (Exception ex) {
                    if (is != null) {
                        is.close();
                    } 
                    LOG.error("Error accessing large binary document " + 
                            key + ", skipping...", ex);
                } 
            } else {
                content = ContentFactory.newContent(uri, 
                        doc.getContentAsByteArray(), options);
            }
        } else if (value instanceof BytesWritable) {
            if (formatNeeded) {
                options.setFormat(DocumentFormat.BINARY);
                formatNeeded = false;
            }
            content = ContentFactory.newContent(uri, 
                    ((BytesWritable) value).getBytes(), 0, 
                    ((BytesWritable) value).getLength(), options);               
        } else if (value instanceof CustomContent) { 
            ContentCreateOptions newOptions = options;
            if (batchSize > 1) {
                newOptions = (ContentCreateOptions)options.clone();
            }
            content = ((CustomContent) value).getContent(conf, newOptions, 
                    uri);
        } else if (value instanceof DatabaseDocument) {
            DatabaseDocument doc = (DatabaseDocument)value;
            if (formatNeeded) {
                options.setFormat(doc.getContentType().getDocumentFormat());
                formatNeeded = false;
            }
            options.setEncoding(DEFAULT_OUTPUT_CONTENT_ENCODING);
            if (doc.getContentType() == ContentType.BINARY) {
                content = ContentFactory.newContent(uri, 
                        doc.getContentAsByteArray(), options);
            } else {
                content = ContentFactory.newContent(uri, 
                        doc.getContentAsText().getBytes(), options);
            }
        } else if (value instanceof StreamLocator) {
            Path path = ((StreamLocator)value).getPath();
            if (fs == null) {         
                URI fileUri = path.toUri();
                fs = FileSystem.get(fileUri, conf);
            }
            switch (((StreamLocator)value).getCodec()) {
                case GZIP:
                    InputStream fileIn = fs.open(path);
                    is = new GZIPInputStream(fileIn);
                    break;
                case ZIP:
                    if (is == null) {
                        InputStream zipfileIn = fs.open(path);
                        ZipInputStream zis = new ZipInputStream(zipfileIn);
                        is = new ZipEntryInputStream(zis, path.toString());
                    }
                    break;
                case NONE:
                    is = fs.open(path);
                    break;
                default:
                    LOG.error("Unsupported compression codec: " + 
                        ((StreamLocator)value).getCodec() + " for document " +
                        key);
                    return content;
            }
            if (streaming) {
                content = ContentFactory.newUnBufferedContent(uri, is, 
                        options);
            } else {
                content = ContentFactory.newContent(uri, is, options);
            }

        } else {
            throw new UnsupportedOperationException(value.getClass()
                    + " is not supported.");
        }
        return content;
    }

    /**
     * Insert batch, log errors and update stats.
     * 
     * @param batch batch of content to insert
     * @param fId forest Id
     */
    protected void insertBatch(Content[] batch, int id) 
    throws IOException {
        try {
            List<RequestException> errors = 
                    sessions[id].insertContentCollectErrors(batch);
            if (errors != null) {
                for (RequestException ex : errors) {
                    Throwable cause = ex.getCause();
                    if (cause != null) {
                        if (cause instanceof XQueryException) {
                            LOG.error(
                                ((XQueryException)cause).getFormatString());
                        } else {
                            LOG.error(cause.getMessage());
                        }
                    }
                    if (ex instanceof ContentInsertException) {
                        Content content = 
                                ((ContentInsertException)ex).getContent();
                        if (!needCommit &&
                            batch[batch.length-1] == content) {
                            failed += batch.length;   
                            // remove the failed content from pendingUris
                            for (Content fc : batch) {
                                DocumentURI failedUri = 
                                        pendingUris[id].remove(fc);
                                LOG.warn("Failed document " + failedUri);
                            }
                        } else {
                            DocumentURI failedUri = 
                                    pendingUris[id].remove(content);
                            failed++;
                            LOG.warn("Failed document " + failedUri);
                        }
                    }
                }
            }
        } catch (RequestServerException e) {
            // log error and continue on RequestServerException
            if (e instanceof XQueryException) {
                LOG.error(((XQueryException)e).getFormatString());
            } else {
                LOG.error(e.getMessage());
            }
            failed += batch.length;   
            // remove the failed content from pendingUris
            for (Content fc : batch) {
                DocumentURI failedUri = pendingUris[id].remove(fc);
                LOG.warn("Failed document " + failedUri);
            }
        } catch (RequestException e) {
            if (sessions[id] != null) {
                sessions[id].close();
            }
            if (countBased) {
                rollbackCount(id);
            }
            pendingUris[id].clear();
            throw new IOException(e);
        }
        if (needCommit) {
            // move uris from pending to commit
            for (DocumentURI uri : pendingUris[id].values()) {
                commitUris[id].add(uri);
            }
        } else {
            succeeded += pendingUris[id].size();
        }
        pendingUris[id].clear();
    }
    
    protected void insertContent(Content content, int id) 
    throws IOException {
        try {
            sessions[id].insertContent(content);
            if (!needCommit) {
                succeeded++;
                pendingUris[id].clear();
            }
        } catch (RequestServerException e) {
            if (e instanceof XQueryException) {
                LOG.error(((XQueryException)e).getFormatString());
            } else {
                LOG.error(e.getMessage());
            }
            failed++;   
            // remove the failed content from pendingUris
            DocumentURI failedUri = pendingUris[id].remove(content.getUri());
            LOG.warn("Failed document " + failedUri);
        } catch (RequestException e) {
            if (sessions[id] != null) {
                sessions[id].close();
            }
            if (countBased) {
                rollbackCount(id);
            }
            failed++;
            pendingUris[id].clear();
            throw new IOException(e);
        }
    }
    
    protected void commit(int id) throws IOException {
        try {
            sessions[id].commit();
            succeeded += commitUris[id].size();
        } catch (RequestServerException e) {
            LOG.error("Error commiting transaction", e);
            failed += commitUris[id].size();   
            for (DocumentURI failedUri : commitUris[id]) {
                LOG.warn("Failed document: " + failedUri);
            }
        } catch (RequestException e) {
            if (sessions[id] != null) {
                sessions[id].close();
            }
            if (countBased) {
                rollbackCount(id);
            }
            failed += commitUris[id].size();
            commitUris[id].clear();
            throw new IOException(e);
        } 
    }
    
    @Override
    public void write(DocumentURI key, VALUEOUT value) 
    throws IOException, InterruptedException {
        InternalUtilities.getUriWithOutputDir(key, outputDir);
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
        Content content = createContent(key,value); 
        if (content == null) {
            failed++;
            return;
        }
        if(countBased) {
            fId = 0;
        }
        pendingUris[sid].put(content, (DocumentURI)key.clone());
        if (batchSize > 1) {
            forestContents[fId][counts[fId]++] = content;
            if (counts[fId] == batchSize) {
                if (sessions[sid] == null) {
                    sessions[sid] = getSession(forestId);
                }  
                insertBatch(forestContents[fId], sid); 
                stmtCounts[sid]++;
                
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
                counts[fId] = 0;
            }
        } else { // batchSize == 1
            if (sessions[sid] == null) {
                sessions[sid] = getSession(forestId);
            }
            insertContent(content, sid);   
            stmtCounts[sid]++;
            //reset forest index for statistical
            if (countBased) {
                sfId = -1;
            }
        }
        if (needCommit && stmtCounts[sid] == txnSize) {
            commit(sid);  
            stmtCounts[sid] = 0;         
            commitUris[sid].clear();
        }
    }

    protected void rollbackCount(int fId) {
        ((StatisticalAssignmentPolicy)am.getPolicy()).updateStats(fId, 
                -batchSize);
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
    
    protected Session getSession(String forestId) {
        TransactionMode mode = TransactionMode.AUTO;
        if (needCommit) {
            mode = TransactionMode.UPDATE;
        }
        return getSession(forestId, mode);
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
                        String forestId = forestIds[sid];
                        sessions[sid] = getSession(forestId);
                    }
                    insertBatch(remainder, sid);   
                    stmtCounts[sid]++;
                }
            }
        }
        for (int i = 0; i < sessions.length; i++) {
            if (sessions[i] != null) {
                if (stmtCounts[i] > 0 && needCommit) {
                    try {
                        sessions[i].commit();
                        succeeded += commitUris[i].size();
                    } catch (RequestServerException e) {
                        // log error and continue on RequestServerException
                        LOG.error("Error commiting transaction", e);
                        failed += commitUris[i].size();   
                        for (DocumentURI failedUri : commitUris[i]) {
                            LOG.warn("Failed document " + failedUri);
                        }
                        commitUris[i].clear();
                    } catch (RequestException e) {
                        if (sessions[i] != null) {
                            sessions[i].close();
                        }
                        if (countBased) {
                            rollbackCount(i);
                        }
                        failed += commitUris[i].size();
                        commitUris[i].clear();
                        throw new IOException(e);
                    } finally {
                        sessions[i].close();
                    }
                } else {
                    sessions[i].close();
                }
            }
        }
        if (is != null) {
            is.close();
            if (is instanceof ZipEntryInputStream) {
                ((ZipEntryInputStream)is).closeZipInputStream();
            }
        }
        context.getCounter(
            ContentPumpStats.OUTPUT_RECORDS_COMMITTED).increment(succeeded);
        context.getCounter(
            ContentPumpStats.OUTPUT_RECORDS_FAILED).increment(failed);
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
    
}
