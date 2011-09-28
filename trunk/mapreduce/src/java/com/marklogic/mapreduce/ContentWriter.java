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
import com.marklogic.xcc.exceptions.RequestException;

/**
 * MarkLogicRecordWriter that inserts content in bytes to MarkLogicServer.
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

    public ContentWriter(Configuration conf, 
            Map<String, ContentSource> forestSourceMap) {
        super(null, conf);
        
        this.forestSourceMap = forestSourceMap;
        forestIds = new String[forestSourceMap.size()];
        forestIds = forestSourceMap.keySet().toArray(forestIds);
        
        this.outputDir = conf.get(OUTPUT_DIRECTORY);
        
        if (batchSize > 1) {
            forestContents = new Content[forestSourceMap.size()][batchSize];
            counts = new int[forestSourceMap.size()];
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
        if (outputDir != null && !outputDir.isEmpty()) {
            uri = outputDir.endsWith("/") || uri.startsWith("/") ? 
                  outputDir + uri : outputDir + '/' + uri;
        }    
        key.setUri(uri);
        DocumentURI.validate(uri);
        int fId = key.getPlacementId(forestIds.length);
        
        String forestId = forestIds[fId];
        Session session = null;
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
                        ((BytesWritable) value).getBytes(), options);
            } else {
                throw new UnsupportedOperationException(value.getClass() + 
                        " is not supported.");
            }
           
            if (batchSize > 1) {
                forestContents[fId][counts[fId]++] = content;
 
                if (counts[fId] == batchSize) {
                    ContentSource cs = forestSourceMap.get(forestId);
                    session = cs.newSession(forestId);
                    session.insertContent(forestContents[fId]);
                    counts[fId] = 0;
                }
            } else {
                ContentSource cs = forestSourceMap.get(forestId);
                session = cs.newSession(forestId);
                session.insertContent(content);
            }
        } catch (RequestException e) {
            LOG.error(e);
            throw new IOException(e);
        } finally {
            if (session != null) {
                session.close();
            } 
        }      
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
        if (forestContents == null) {
            return;
        }       
        for (int i = 0; i < forestIds.length; i++) {
            if (counts[i] > 0) {
                Content[] remainder = new Content[counts[i]];
                System.arraycopy(forestContents[i], 0, remainder, 0, 
                        counts[i]);
                String forestId = forestIds[i];
                ContentSource cs = forestSourceMap.get(forestId);
                Session session = cs.newSession(forestId);
                try {
                    session.insertContent(remainder);
                } catch (RequestException e) {
                    LOG.error(e);
                    throw new IOException(e);
                } finally {
                    if (session != null) {
                        session.close();
                    } 
                } 
            }
        }
    }
}
