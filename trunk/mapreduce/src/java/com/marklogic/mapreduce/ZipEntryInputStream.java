/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ZipEntryInputStream is a wrapper class of ZipInputStream to interface with
 * XCC so that the zip entries can be read sequentially.
 * 
 * @author jchen
 */
public class ZipEntryInputStream extends InputStream {
    public static final Log LOG = LogFactory.getLog(ZipEntryInputStream.class);
    
    private ZipInputStream zipIn;
    private String fileName;
    
    public ZipEntryInputStream(ZipInputStream zipIn, String fileName) {
        this.zipIn = zipIn;   
        this.fileName = fileName;
        // advance the stream to the first zip entry position.
        hasNext();
    }
    
    public boolean hasNext() {
        try {
            return zipIn.getNextEntry() != null;
        } catch (IOException e) {
            LOG.error("Error getting next zip entry from " + fileName,
                    e);
            return false;
        }
    }
    
    @Override
    public int read() throws IOException {  
        int bytes = zipIn.read();
        if (bytes == -1) {
            // advance the stream to the next entry if done with this one.
            zipIn.getNextEntry();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("bytes read from " + fileName + ": " + bytes);
        }
        return bytes;
    }
    
    @Override
    public int read(byte[] buf) throws IOException {
        int bytes = zipIn.read(buf);
        if (bytes == -1) {
            zipIn.getNextEntry();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("bytes read from " + fileName + ": " + bytes);
        }
        return bytes;
    }
    
    @Override 
    public int read(byte[] b, int off, int len) throws IOException {
        int bytes = zipIn.read();
        if (bytes == -1) {
            zipIn.getNextEntry();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("bytes read from " + fileName + ": " + bytes);
        }
        return bytes;
    }  
    
    @Override
    public void close() {
    }
    
    public void closeZipInputStream() throws IOException {
        if (zipIn != null) {
            zipIn.close();
        }
    }
}
