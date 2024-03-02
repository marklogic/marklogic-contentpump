/*
 * Copyright (c) 2023 MarkLogic Corporation
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
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author mattsun
 *
 */
public class ZipDelimitedJSONReader extends DelimitedJSONReader<Text> {
    public static final Log LOG = 
            LogFactory.getLog(ZipDelimitedJSONReader.class);  
    private byte[] buf = new byte[65536];
    private InputStream zipIn;
    private ZipEntry currZipEntry;
    
    @Override
    public void close() throws IOException {
        super.close();
        if (zipIn != null) {
            zipIn.close();
        }
    }
    
    @Override
    protected void initFileStream(InputSplit inSplit) 
            throws IOException, InterruptedException {
        fileIn = openFile(inSplit, false);
        if (fileIn == null) {
            return;
        }
        zipIn = new ZipInputStream(fileIn);
    }
    
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        super.initialize(inSplit, context);
        findNextFileEntryAndInitReader();
    }
    
    @Override
    protected boolean findNextFileEntryAndInitReader() throws InterruptedException, IOException {
        do {
            if (hasMoreEntryInZip()) return true;
            if (iterator != null && iterator.hasNext()) {
                initFileStream(iterator.next());
                continue;
            } else {
                hasNext = false;
                return false;
            }
        } while (true);
    }
    
    protected boolean hasMoreEntryInZip() throws IOException {
        ByteArrayOutputStream byteArrayOStream;
        ZipInputStream zipIStream = (ZipInputStream) zipIn;
        
        while ((currZipEntry = zipIStream.getNextEntry()) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ZipEntry: " + currZipEntry.getName());
            }
            if (currZipEntry.getSize() == 0) {
                continue;
            }
            subId = currZipEntry.getName();
            configFileNameAsCollection(conf, file);
            instream = new InputStreamReader(zipIStream, encoding);
            reader = new LineNumberReader(instream);        
            return true;
        }   
        return false;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext?0:1;
    }
 
    @Override
    protected void configFileNameAsCollection(Configuration conf, Path file) {
        String collectionName = file.getName() + "_" + currZipEntry.getName();
        super.configFileNameAsCollection(conf, new Path(collectionName));
    }
    
}
