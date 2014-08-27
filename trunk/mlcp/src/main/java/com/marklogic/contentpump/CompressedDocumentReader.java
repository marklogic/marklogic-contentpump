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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.mapreduce.CompressionCodec;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * RecordReader for CompressedDocumentInputFormat.
 * 
 * @author ali
 * 
 * @param <VALUEIN>
 */
public class CompressedDocumentReader<VALUEIN> extends
    ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(
            CompressedDocumentReader.class);
    protected InputStream zipIn;
    protected byte[] buf = new byte[65536];
    protected boolean hasNext = true;
    protected CompressionCodec codec;
    protected int batchSize;
    public CompressedDocumentReader() {

    }

    @Override
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing " + file);
        }
        if (zipIn != null) {
            zipIn.close();
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initConfig(context);
        batchSize = conf.getInt(MarkLogicConstants.BATCH_SIZE, 
            MarkLogicConstants.DEFAULT_BATCH_SIZE);
        file = ((FileSplit) inSplit).getPath();  
        fs = file.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(file);
        if(status.isDir()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        initStream(inSplit);
    }
    
    protected void initStream(InputSplit inSplit) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting " + file);
        }
        file = ((FileSplit) inSplit).getPath();  

        FSDataInputStream fileIn = fs.open(file);

        String codecString = conf.get(
            ConfigConstants.CONF_INPUT_COMPRESSION_CODEC,
            CompressionCodec.ZIP.toString()).toUpperCase();
        codec = CompressionCodec.valueOf(codecString);
        switch (codec) {
        case ZIP:
            zipIn = new ZipInputStream(fileIn);
            break;
        case GZIP:
            zipIn = new GZIPInputStream(fileIn);
            String uri = makeURIFromPath(file);
            if (uri == null) {
                key = null;
            } else {
                if (uri.toLowerCase().endsWith(".gz") || 
                    uri.toLowerCase().endsWith(".gzip")) {
                    uri = uri.substring(0, uri.lastIndexOf('.'));
                } 
                setKey(uri);
            }
            break;
        default:
            String error = "Unsupported codec: " + codec.name();
            LOG.error(error, new UnsupportedOperationException(error));
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        if (codec == CompressionCodec.ZIP) {
            ZipEntry zipEntry;
            ZipInputStream zis = (ZipInputStream) zipIn;
            while ((zipEntry = zis.getNextEntry()) != null) {
                if (zipEntry != null && zipEntry.getSize() != 0) {
                    String uri = makeURIForZipEntry(file, zipEntry.getName());
                    if (uri != null) {
                        setKey(uri);
                        setValue(zipEntry.getSize());
                    } else {
                        key = null;
                    }
                    return true;
                }
            }
        } else if (codec == CompressionCodec.GZIP) {
            setValue(0);
            zipIn.close();
            zipIn = null;
            hasNext = false;
            return true;
        } else {
            throw new UnsupportedOperationException("Unsupported codec: "
                + codec.name());
        }
        
        if (iterator != null && iterator.hasNext()) {
            close();
            initStream(iterator.next());
            return nextKeyValue();
        } else {
            hasNext = false;
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    protected void setValue(long length) throws IOException {
        ByteArrayOutputStream baos;
        if (length > 0) {
            baos = new ByteArrayOutputStream((int) length);
        } else {
            baos = new ByteArrayOutputStream();
        }
         
        int size;
        while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, size);
        }
        if (value instanceof Text) {
            ((Text) value).set(baos.toString(encoding));
        } else if (value instanceof BytesWritable) {
            if (batchSize > 1) {
                // Copy data since XCC won't do it when Content is created.
                value = (VALUEIN) new BytesWritable();
            }
            ((BytesWritable) value).set(baos.toByteArray(), 0, baos.size());
        } else {
            String error = "Unsupported input value class: " + 
                value.getClass();
            LOG.error(error, new UnsupportedOperationException(error)); 
            key = null;
        }
        baos.close();
    }
}
