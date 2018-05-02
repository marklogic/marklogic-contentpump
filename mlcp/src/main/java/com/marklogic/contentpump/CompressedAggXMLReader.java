/*
 * Copyright 2003-2018 MarkLogic Corporation
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

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.contentpump.utilities.ZipInputStream;
import com.marklogic.mapreduce.CompressionCodec;

/**
 * Reader for CompressedAggXMLInputFormat.
 * 
 * @author ali
 *
 * @param <VALUEIN>
 */
public class CompressedAggXMLReader<VALUEIN> extends
    AggregateXMLReader<VALUEIN> {
    public static final Log LOG = LogFactory
        .getLog(CompressedAggXMLReader.class);
    private InputStream zipIn;
    private ZipEntry currZipEntry;
    private CompressionCodec codec;

    @Override
    public void close() throws IOException {
        super.close();
        //close the zip
        if (zipIn != null) {
            if (zipIn instanceof ZipInputStream) {
                ((ZipInputStream)zipIn).closeStream();
            }
            zipIn.close();
        }
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initConfig(context);
        initAggConf(context);
        f = XMLInputFactory.newInstance();
        setFile(((FileSplit) inSplit).getPath());
        fs = file.getFileSystem(context.getConfiguration());

        FileStatus status = fs.getFileStatus(file);
        if(status.isDirectory()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        initStreamReader(inSplit);
    }

    protected void initStreamReader(InputSplit inSplit) throws IOException,
    InterruptedException {
        FSDataInputStream fileIn = openFile(inSplit, false);
        if (fileIn == null) {
            return;
        }
        String codecString = conf.get(
            ConfigConstants.CONF_INPUT_COMPRESSION_CODEC,
            CompressionCodec.ZIP.toString());
        if (codecString.equalsIgnoreCase(CompressionCodec.ZIP.toString())) {
            zipIn = new ZipInputStream(fileIn);
            codec = CompressionCodec.ZIP;
            while (true) {
                try {
                    currZipEntry = ((ZipInputStream)zipIn).getNextEntry();
                    if (currZipEntry == null) {
                        break;
                    }
                    if (currZipEntry.getSize() != 0) {
                        subId = currZipEntry.getName();
                        break;
                    }
                } catch (IllegalArgumentException e) {
                    LOG.warn("Skipped a zip entry in : " + file.toUri()
                            + ", reason: " + e.getMessage());
                }
            }
            if (currZipEntry == null) { // no entry in zip
                LOG.warn("No valid entry in zip:" + file.toUri());
                return;
            }
            try {
                start = 0;
                end = currZipEntry.getSize();
                xmlSR = f.createXMLStreamReader(zipIn, encoding);
            } catch (XMLStreamException e) {
                LOG.error(e.getMessage(), e);
            }

        } else if (codecString.equalsIgnoreCase(CompressionCodec.GZIP
            .toString())) {
            zipIn = new GZIPInputStream(fileIn);
            codec = CompressionCodec.GZIP;
            try {
                start = 0;
                end = inSplit.getLength();
                xmlSR = f.createXMLStreamReader(zipIn, encoding);
            } catch (XMLStreamException e) {
                LOG.error(e.getMessage(), e);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported codec: "
                + codecString);
        }    
        if (useAutomaticId) {
            idGen = new IdGenerator(file.toUri().getPath() + "-"
                + ((FileSplit)inSplit).getStart());
        }
    }
    
    private boolean nextRecordInAggregate() throws IOException,
        XMLStreamException, InterruptedException {
        return super.nextKeyValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null || xmlSR == null) {
            hasNext = false;
            return false;
        }
        try {
            if (codec.equals(CompressionCodec.ZIP)) {
                if (xmlSR.hasNext()) {
                    hasNext = nextRecordInAggregate();
                    if (hasNext) {
                        return true;
                    }
                }
                // xmlSR does not hasNext, 
                // if there is next zipEntry, close xmlSR then create a new one
                while (true) {
                    try {
                        currZipEntry = ((ZipInputStream)zipIn).getNextEntry();
                        
                        if (currZipEntry == null) {
                            break;
                        }
                        if (currZipEntry.getSize() == 0) {
                            continue;
                        }
                        subId = currZipEntry.getName();
                        xmlSR.close();
                        start = 0;
                        end = currZipEntry.getSize();
                        xmlSR = f.createXMLStreamReader(zipIn, encoding);
                        nameSpaces.clear();
                        return nextRecordInAggregate();
                    } catch (IllegalArgumentException e) {
                        LOG.warn("Skipped a zip entry in : " + file.toUri()
                                + ", reason: " + e.getMessage());
                    }
                }
                // end of zip
                if (iterator != null && iterator.hasNext()) {
                    //close previous zip and streams
                    close();
                    initStreamReader(iterator.next());
                    return nextRecordInAggregate();
                }
                //current split doesn't have more zip
                return false;

            } else if (codec.equals(CompressionCodec.GZIP)) {
                 if(nextRecordInAggregate()) {
                     return true;
                 }
                 // move to next gzip file in this split
                 if (iterator != null && iterator.hasNext()) {
                     // close previous zip and streams
                     close();
                     initStreamReader(iterator.next());
                     return nextRecordInAggregate();
                 }
              	 return false;
            }
        } catch (XMLStreamException e) {
            LOG.error(e.getMessage(), e);
        }
        return true;
    }
    public CompressedAggXMLReader() {
        super();
        compressed = true;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }

}
