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
 * XCC closes the input stream after reading from it once.  This wrapper class 
 * blocks the close call since the ZipInputStream needs to be read repeatedly.
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
        return zipIn.read();
    }
    
    @Override
    public int read(byte[] buf) throws IOException {
        return zipIn.read(buf);
    }
    
    @Override 
    public int read(byte[] b, int off, int len) throws IOException {
        return zipIn.read();
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
