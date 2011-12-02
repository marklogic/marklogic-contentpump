/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
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
     * An array of forest ids
     */
    private String[] forestIds;

    /**
     * Whether in fast load mode.
     */
    private boolean fastLoad;
    
    /**
     * An array of record counts per forest.
     */
    private int counts[];
    
    /**
     * An array of sessions per forest.
     */
    private Session sessions[];

    public ContentWriter(Configuration conf, 
            Map<String, ContentSource> forestSourceMap, boolean fastLoad) {
        super(null, conf);
        this.fastLoad = fastLoad;
        
        this.forestSourceMap = forestSourceMap;
        forestIds = new String[forestSourceMap.size()];
        forestIds = forestSourceMap.keySet().toArray(forestIds);
        
        this.outputDir = conf.get(OUTPUT_DIRECTORY);
        
        if (batchSize > 1) {
            counts = new int[forestIds.length];
        }
        
        sessions = new Session[forestIds.length];
        
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
            options.setPermissions(permissions.toArray(new ContentPermission[permissions.size()]));
        } 
        String contentTypeStr = conf.get(CONTENT_TYPE, DEFAULT_CONTENT_TYPE);
        ContentType contentType = ContentType.valueOf(contentTypeStr);
        options.setFormat(contentType.getDocumentFormat());
    }

    @Override
    public void write(DocumentURI key, VALUEOUT value) 
    throws IOException, InterruptedException {
        String uri = key.getUri();
        String forestId = ContentOutputFormat.ID_PREFIX; 
        int fId = 0;
        if (fastLoad) { // compute forest to write to
            // outputDir can only be set if fastLoad is true
            if (outputDir != null && !outputDir.isEmpty()) {
                uri = outputDir.endsWith("/") || uri.startsWith("/") ? 
                      outputDir + uri : outputDir + '/' + uri;
            }    
            key.setUri(uri);
            DocumentURI.validate(uri);
            fId = key.getPlacementId(forestIds.length);
            
            forestId = forestIds[fId];
        } else {
            DocumentURI.validate(uri);
        }
        
        try {
            Content content = null;
            if (value instanceof Text) {
                content = ContentFactory.newContent(uri, 
                        ((Text)value).toString(), options);
            } else if (value instanceof MarkLogicNode) {
                content = ContentFactory.newContent(uri, 
                        ((MarkLogicNode)value).get(), options);   
            } else if (value instanceof BytesWritable) {
                content = ContentFactory.newContent(uri, 
                        ((BytesWritable) value).getBytes(), 0, 
                        ((BytesWritable) value).getLength(), options);
            } else {
                throw new UnsupportedOperationException(value.getClass() + 
                        " is not supported.");
            }
            
            if (batchSize > 1) {
                if (sessions[fId] == null) {
                    ContentSource cs = forestSourceMap.get(forestId);
                    sessions[fId] = getSession(cs, forestId, 
                            TransactionMode.UPDATE); 
                }
                        
                sessions[fId].insertContent(content);
                counts[fId]++;
                if (counts[fId] == batchSize) {
                    sessions[fId].commit();
                    counts[fId] = 0;
                }
            } else {
                if (sessions[fId] == null) {
                    ContentSource cs = forestSourceMap.get(forestId);
                    sessions[fId] = getSession(cs, forestId, 
                            TransactionMode.AUTO); 
                }  
                sessions[fId].insertContent(content);   
            }
        } catch (RequestException e) {
            LOG.error(e);
            throw new IOException(e);
        }  
    }

    private Session getSession(ContentSource cs, String forestId, 
            TransactionMode mode) {
        Session session;
        if (fastLoad) {
            session = cs.newSession(forestId);         
        } else {
            session = cs.newSession();
        }      
        session.setTransactionMode(mode);
        return session;
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {      
        for (int i = 0; i < sessions.length; i++) {
            if (counts != null && counts[i] > 0) {
                try {
                    sessions[i].commit();
                } catch (RequestException e) {
                    LOG.error(e);
                    throw new IOException(e);
                }
            } 
            if (sessions[i] != null) {
                sessions[i].close();
            }
        }
    }
}
