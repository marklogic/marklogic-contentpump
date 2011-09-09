/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
     * A map from a forest id to a Session.
     */
    private Map<String, List<Content>> forestContentMap;
    
    /**
     * An array of forest ids
     */
    private String[] forestIds;

    public ContentWriter(Configuration conf, 
            Map<String, ContentSource> forestSourceMap) {
        super(null, conf);
        
        this.forestSourceMap = forestSourceMap;
        forestIds = new String[forestSourceMap.size()];
        forestIds = forestSourceMap.keySet().toArray(forestIds);
        
        this.outputDir = conf.get(OUTPUT_DIRECTORY);
        
        if (batchSize > 1) {
            forestContentMap = new HashMap<String, List<Content>>();
        }
        
        String[] perms = conf.getStrings(OUTPUT_PERMISSION);
        List<ContentPermission> permissions = null;
        if (perms != null && perms.length > 0) {
            int i = 0;
            while (i + 1 < perms.length) {
                String roleName = perms[i++];
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
        } else {
            LOG.debug("no permissions");
        }
        
        options = new ContentCreateOptions();
        options.setCollections(conf.getStrings(OUTPUT_COLLECTION));
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
        int fId = key.getPlacementId(forestIds.length);
        String forestId = forestIds[fId];
        ContentSource cs = forestSourceMap.get(forestId);
        Session session = null;
        try {
            String uri = key.getUri();
            if (outputDir != null && !outputDir.isEmpty()) {
                uri = outputDir.endsWith("/") || uri.startsWith("/") ? 
                      outputDir + uri : outputDir + '/' + uri;
            }    
            DocumentURI.validate(uri);
            Content content = null;
            if (value instanceof Text) {
                content = ContentFactory.newContent(uri, 
                        ((Text)value).toString(), options);
            } else if (value instanceof MarkLogicNode) {
                content = ContentFactory.newContent(uri, 
                        ((MarkLogicNode)value).get(), options);        
            } else {
                throw new UnsupportedOperationException(value.getClass() + 
                        " is not supported.");
            }
            session = cs.newSession(forestId);
            if (batchSize > 1) {
                List<Content> contentList = forestContentMap.get(forestId);
                if (contentList == null) {
                    contentList = new ArrayList<Content>();
                    forestContentMap.put(forestId, contentList);
                }
                contentList.add(content);
                if (contentList.size() == batchSize) {
                    Content[] contents = contentList.toArray(
                            new Content[batchSize]);
                    session.insertContent(contents);
                    contentList.clear();
                }
            } else {
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
        if (forestContentMap == null) {
            return;
        }       
        for (String forestId : forestContentMap.keySet()) {
            List<Content> contentList = forestContentMap.get(forestId);
            if (contentList != null && !contentList.isEmpty()) {
                Session session = 
                    forestSourceMap.get(forestId).newSession(forestId);
                Content[] contents = contentList.toArray(
                        new Content[contentList.size()]);
                try {
                    session.insertContent(contents);
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
