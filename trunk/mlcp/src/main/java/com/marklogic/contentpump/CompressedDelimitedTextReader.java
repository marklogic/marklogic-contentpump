/*
 * Copyright 2003-2013 MarkLogic Corporation
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.mapreduce.CompressionCodec;

/**
 * Reader for CompressedDelimitedTextInputFormat.
 * @author ali
 *
 */
public class CompressedDelimitedTextReader extends DelimitedTextReader<Text> {
    public static final Log LOG = LogFactory
        .getLog(CompressedDelimitedTextReader.class);
    private byte[] buf = new byte[65536];
    private InputStream zipIn;
    private ZipEntry currZipEntry;
    private CompressionCodec codec;

    public CompressedDelimitedTextReader() {
        super();
        compressed = true;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initConfig(context);
        initDelimConf();
        
        file = ((FileSplit) inSplit).getPath();
        fs = file.getFileSystem(context.getConfiguration());
        FileStatus status = fs.getFileStatus(file);
        if(status.isDir()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        
        initStream(inSplit);
    }
    
    protected void initStream(InputSplit inSplit) throws IOException {
        file = ((FileSplit) inSplit).getPath();
        FSDataInputStream fileIn = fs.open(file);
        
        String codecString = conf.get(
            ConfigConstants.CONF_INPUT_COMPRESSION_CODEC,
            CompressionCodec.ZIP.toString());
        if (codecString.equalsIgnoreCase(CompressionCodec.ZIP.toString())) {
            zipIn = new ZipInputStream(fileIn);
            codec = CompressionCodec.ZIP;
        } else if (codecString.equalsIgnoreCase(CompressionCodec.GZIP
            .toString())) {
            zipIn = new GZIPInputStream(fileIn);
            codec = CompressionCodec.GZIP;
        }
        
        if (uriName == null) {
            generateId = conf.getBoolean(CONF_DELIMITED_GENERATE_URI, false);
            if (generateId) {
                idGen = new IdGenerator(file.toUri().getPath() + "-"
                    + ((FileSplit) inSplit).getStart());
            } else {
                uriId = 0;
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        if (instream == null) {
            if (codec.equals(CompressionCodec.ZIP)) {
                return nextKeyValueInZip();
            } else if (codec.equals(CompressionCodec.GZIP)) {
                instream = new InputStreamReader(zipIn, encoding);
                parser = new CSVParser(instream, new CSVStrategy(delimiter,
                    encapsulator, CSVStrategy.COMMENTS_DISABLED,
                    CSVStrategy.ESCAPE_DISABLED, true, true, false, true));
                return super.nextKeyValue();
            } else {
                throw new UnsupportedOperationException("Unsupported codec: "
                    + codec.name());
            }
        } else {
            if (codec.equals(CompressionCodec.ZIP)) {
                if (super.nextKeyValue()) {
                    // current delim txt has next
                    return true;
                }
                return nextKeyValueInZip();
            } else if (codec.equals(CompressionCodec.GZIP)) {
                if (super.nextKeyValue()) {
                    return true;
                }
                // move to next gzip file in this split
                if (iterator != null && iterator.hasNext()) {
                    // close previous zip and streams
                    close();
                    initStream(iterator.next());
                    if (encoding == null) {
                        instream = new InputStreamReader(zipIn);
                    } else {
                        instream = new InputStreamReader(zipIn, encoding);
                    }
                    parser = new CSVParser(instream, new CSVStrategy(
                        delimiter, encapsulator,
                        CSVStrategy.COMMENTS_DISABLED,
                        CSVStrategy.ESCAPE_DISABLED, true, true, false, true));
                    return super.nextKeyValue();
                } else
                    return false;
            } else {
                throw new UnsupportedOperationException("Unsupported codec: "
                    + codec.name());
            }
        }
    }
    
    private boolean nextKeyValueInZip() throws IOException, InterruptedException{
        ByteArrayOutputStream baos;
        ZipInputStream zis = (ZipInputStream) zipIn;
        
        while ((currZipEntry = zis.getNextEntry()) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ZipEntry: " + currZipEntry.getName());
            }
            if (currZipEntry.getSize() == 0) {
                continue;
            }
            long size = currZipEntry.getSize();
            if (size == -1) {
                baos = new ByteArrayOutputStream();
            } else {
                baos = new ByteArrayOutputStream((int) size);
            }
            int nb;
            while ((nb = zis.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, nb);
            }
            if (encoding == null) {
                instream = new InputStreamReader(
                    new ByteArrayInputStream(baos.toByteArray()));
            } else {
                instream = new InputStreamReader(
                    new ByteArrayInputStream(baos.toByteArray()), encoding);
            }
            baos.close();
            parser = new CSVParser(instream, new CSVStrategy(delimiter,
                encapsulator, CSVStrategy.COMMENTS_DISABLED,
                CSVStrategy.ESCAPE_DISABLED, true, true, false, true));
            //clear metadata
            fields = null;
            if (super.nextKeyValue()) {
                // current delim txt has next
                return true;
            }
            // continue read next zip entry if any
        }
        // end of zip
        if (iterator != null && iterator.hasNext()) {
            close();
            initStream(iterator.next());
            return nextKeyValueInZip();
        } else {
            hasNext = false;
            return false;
        }
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        if (zipIn != null) {
            zipIn.close();
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }
}
