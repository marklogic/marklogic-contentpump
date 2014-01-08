/*
 * Copyright 2003-2014 MarkLogic Corporation
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Archive for export, create zip file(s).
 * @author ali
 *
 */
public class OutputArchive {
    public static final Log LOG = LogFactory.getLog(OutputArchive.class);
    public static String EXTENSION = ".zip";
    private long currentFileBytes = 0;
    private ZipOutputStream outputStream;
    private String basePath;
    private String currPath;
    private static AtomicInteger fileCount = new AtomicInteger();
    private int currentEntries;
    private Configuration conf;
    
    public OutputArchive(String path, Configuration conf) {
        if (path.toLowerCase().endsWith(EXTENSION)) {
            this.basePath = path;
        } else {
            this.basePath = path + EXTENSION;
        }
        this.conf = conf;
    }

    private void newOutputStream() throws IOException {
        // use the constructor filename for the first zip,
        // then add filecount to subsequent archives, if any.
        int count = fileCount.getAndIncrement();
        currPath = newPackagePath(basePath, count, 6);
        if (outputStream != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("closing output archive: " + currPath);
            }   
            outputStream.flush();
            outputStream.close();
        }
        currentFileBytes = 0;
        currentEntries = 0;

        Path zpath = new Path(currPath);
        FileSystem fs = zpath.getFileSystem(conf);
        if (fs.exists(zpath)) {
            throw new IOException(zpath + " already exists.");
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating output archive: " + zpath);
            LOG.debug("Default charset: " + Charset.defaultCharset());
        }
        // if fs instanceof DistributedFileSystem, use hadoop api; otherwise,
        // use java api
        if (fs instanceof DistributedFileSystem) {
            FSDataOutputStream fsout = fs.create(zpath, false);
            outputStream = new ZipOutputStream(fsout);
        } else {
            File f = new File(zpath.toUri().getPath());
            if (!f.exists()) {
                f.getParentFile().mkdirs();
                f.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(f, false);
            outputStream = new ZipOutputStream(fos);
        }

    }

    /**
     * @param canonicalPath
     * @param count
     * @param width
     * @return
     */
    static protected String newPackagePath(String canonicalPath, int count,
        int width) {
        String path = canonicalPath;
        if (path.endsWith(EXTENSION)) {
            int index1 = path.lastIndexOf(EXTENSION);
            String subStr = path.substring(0, index1);
            int index2 = subStr.lastIndexOf('-');
            path = path.substring(0, index2)
                + String.format("-%0" + width + "d", count)
                + path.substring(index2);
        } else {
            path = path + "-" + count;
        }
        return path;
    }
    
    public void write(String uri, InputStream is, long size) 
    throws IOException {
        ZipEntry entry = new ZipEntry(uri);
        if (outputStream == null || 
            (currentFileBytes + size > Integer.MAX_VALUE) &&
             currentFileBytes > 0) {
            newOutputStream();
        }        
        try {
            outputStream.putNextEntry(entry);
            long bufSize = Math.min(size, 512<<10);
            byte[] buf = new byte[(int)bufSize];
            for (long toRead = size, read = 0; toRead > 0; toRead -= read) {
                read = is.read(buf, 0, (int)bufSize);
                if (read > 0) {
                    outputStream.write(buf, 0, (int)read);
                }
            }
            outputStream.closeEntry();
        } catch (ZipException e) {
            LOG.warn("Exception caught: " + e.getMessage() + entry.getName());
        }
        currentFileBytes += size;
        currentEntries++;
    }

    public long write(String outputPath, byte[] bytes) throws IOException {

        if (null == outputPath) {
            throw new NullPointerException("null path");
        }
        if (null == bytes) {
            throw new NullPointerException("null content bytes");
        }

        long total = bytes.length;
        ZipEntry entry = new ZipEntry(outputPath);

        if (outputStream == null) {
            newOutputStream();
        }

        if (currentFileBytes > 0
            && currentFileBytes + total > Integer.MAX_VALUE) {
            if (currentEntries % 2 ==0) {
            	//the file overflowed is metadata, create new zip
                newOutputStream();
            } else {
            	//the file overflowed is doc, keep it in current zip
                LOG.warn("too many bytes in current package:" + currPath);
            }
        }

        try {
            outputStream.putNextEntry(entry);
            outputStream.write(bytes);
            outputStream.closeEntry();
        } catch (ZipException e) {
            LOG.warn("Exception caught: " + e.getMessage() + entry.getName());
            return 0;
        }

        currentFileBytes += total;
        currentEntries++;

        return total;

    }

    public void close() throws IOException {
        if (outputStream != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("closing output archive: " + currPath);
            }           
            outputStream.flush();
            outputStream.close();
        }
    }

}
