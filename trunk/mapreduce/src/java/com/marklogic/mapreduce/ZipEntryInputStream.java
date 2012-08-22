/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
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
    private String entryName;
    
    public ZipEntryInputStream(ZipInputStream zipIn, String fileName) {
        this.zipIn = zipIn;   
        this.fileName = fileName;
        // advance the stream to the first zip entry position.
        hasNext();
    }
    
    public boolean hasNext() {
        try {        
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                if (entry.getSize() > 0) {
                    entryName = entry.getName();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Zip entry name: " + entryName);
                    }
                    return true;
                }    
            }
            return false;
        } catch (IOException e) {
            LOG.error("Error getting next zip entry from " + fileName, e);
            return false;
        }
    }
    
    @Override
    public int read() throws IOException {  
        int bytes = zipIn.read();
        if (bytes == -1) {
            // advance the stream to the next entry if done with this one.
            hasNext();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("bytes read from " + fileName + " " + entryName + 
                    ": " + bytes);
        }
        return bytes;
    }
    
    @Override
    public int read(byte[] buf) throws IOException {
        int bytes = zipIn.read(buf);
        if (bytes == -1) {
            hasNext();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("bytes read from " + fileName + " " + entryName + 
                    ": " + bytes);
        }
        return bytes;
    }
    
    @Override 
    public int read(byte[] b, int off, int len) throws IOException {
        int bytes = zipIn.read();
        if (bytes == -1) {
            hasNext();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("bytes read from " + fileName + " " + entryName + 
                    ": " + bytes);
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
