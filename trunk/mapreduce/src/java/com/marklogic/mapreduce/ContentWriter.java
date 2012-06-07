/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
import com.marklogic.xcc.exceptions.RequestException;

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
    
    private boolean formatNeeded;

    public ContentWriter(Configuration conf, 
            Map<String, ContentSource> forestSourceMap, boolean fastLoad) {
        super(null, conf);
        
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
        options.setEncoding(conf.get(OUTPUT_CONTENT_ENCODING,
                DEFAULT_OUTPUT_CONTENT_ENCODING));
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
    }

    @Override
    public void write(DocumentURI key, VALUEOUT value) 
    throws IOException, InterruptedException {
        String uri = key.getUri();
        String forestId = ContentOutputFormat.ID_PREFIX;
        int fId = 0;
        if (fastLoad) {
            // compute forest to write to 
            if (outputDir != null && !outputDir.isEmpty()) {
                uri = outputDir.endsWith("/") || uri.startsWith("/") ? 
                      outputDir + uri : outputDir + '/' + uri;
            }    
            key.setUri(uri);
            DocumentURI.validate(uri);
            fId = key.getPlacementId(forestIds.length);
            
            forestId = forestIds[fId];
        }
 
        try {
            Content content = null;
            if (value instanceof Text) {
                if (formatNeeded) {
                    options.setFormat(DocumentFormat.TEXT);
                    formatNeeded = false;
                }
                content = ContentFactory.newContent(uri, 
                        ((Text)value).toString(), options);             
            } else if (value instanceof MarkLogicNode) {
                if (formatNeeded) {
                    options.setFormat(DocumentFormat.XML);
                    formatNeeded = false;
                }
                content = ContentFactory.newContent(uri, 
                        ((MarkLogicNode)value).get(), options);                 
            } else if (value instanceof BytesWritable) {
                if (formatNeeded) {
                    options.setFormat(DocumentFormat.BINARY);
                    formatNeeded = false;
                }
                content = ContentFactory.newContent(uri, 
                        ((BytesWritable) value).getBytes(), 0, 
                        ((BytesWritable) value).getLength(), options);               
            } else if (value instanceof CustomContent) {
                content = ((CustomContent) value).getContent(conf, options, uri);
                batchSize = 1;
            } else if (value instanceof MarkLogicDocument) {
                MarkLogicDocument doc = (MarkLogicDocument)value;
                if (formatNeeded) {
                    options.setFormat(doc.getContentType().getDocumentFormat());
                    formatNeeded = false;
                }
                if (doc.getContentType() == ContentType.BINARY) {
                    content = ContentFactory.newContent(uri, 
                              doc.getContentAsByteArray(), options);
                } else {
                    content = ContentFactory.newContent(uri, 
                              doc.getContentAsText().toString(), options);
                }              
            } else {
                throw new UnsupportedOperationException(value.getClass()
                    + " is not supported.");
            }
            if (batchSize > 1) {
                forestContents[fId][counts[fId]++] = content;
 
                if (counts[fId] == batchSize) {                
                    if (sessions[fId] == null) {
                        sessions[fId] = getSession(forestId);
                    }        
                    sessions[fId].insertContent(forestContents[fId]);
                    stmtCounts[fId]++;
                    counts[fId] = 0;
                }
            } else {
                if (sessions[fId] == null) {
                    sessions[fId] = getSession(forestId);
                }            
                sessions[fId].insertContent(content);
                stmtCounts[fId]++;
            }
            if (txnSize > 1 && stmtCounts[fId] == txnSize) {
                sessions[fId].commit();
                stmtCounts[fId] = 0;
            }
        } catch (RequestException e) {
            LOG.error(e);
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
        if (txnSize > 1) {
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
                        sessions[i].insertContent(remainder);
                        stmtCounts[i]++;        
                    } catch (RequestException e) {
                        LOG.error(e);
                        if (sessions[i] != null) {
                            sessions[i].close();
                        }
                        throw new IOException(e);
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
            return conf.getInt(TXN_SIZE, 0);
        } 
        return 1000 / conf.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }
}
