/*
 * Copyright 2003-2012 MarkLogic Corporation
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
import java.io.StringReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.MarkLogicDocument;

/**
 * Read archive, construct MarkLogicDocumentWithMeta as value
 * @author ali
 *
 */
public class ArchiveRecordReader extends
    AbstractRecordReader<MarkLogicDocumentWithMeta> implements 
    ConfigConstants {
    private ZipInputStream zipIn;
    private boolean hasNext = true;
    private static String EXTENSION = ".zip";
    private byte[] buf = new byte[65536];
    private boolean allowEmptyMeta = false;
    private int count = 0;
    /**
     * the type of files in this archive Valid choices: XML, TEXT, BINARY
     */
    private ContentType type;

    public ArchiveRecordReader() {
    }

    @Override
    public void close() throws IOException {
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
        Configuration conf = context.getConfiguration();
        Path file = ((FileSplit) inSplit).getPath();
        int index = file.toUri().getPath().lastIndexOf(EXTENSION);
        if (index == -1) {
            throw new IOException("Archive file should have suffix .zip");
        }
        String subStr = file.toUri().getPath().substring(0, index);
        index = subStr.lastIndexOf('.');
        if (index == -1) {
            throw new IOException("Not type information in Archive name");
        }
        String typeStr = subStr.substring(index + 1, subStr.length());
        type = ContentType.valueOf(typeStr);
        initCommonConfigurations(conf, file);
        value = new MarkLogicDocumentWithMeta();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        zipIn = new ZipInputStream(fileIn);

        allowEmptyMeta = conf.getBoolean(
                        CONF_INPUT_ARCHIVE_ALLOW_EMPTY_METADATA, false);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }

        ZipEntry zipEntry;
        ZipInputStream zis = (ZipInputStream) zipIn;
        while ((zipEntry = zis.getNextEntry()) != null) {
            if (zipEntry.isDirectory()) {
                continue;
            }
            if (zipEntry != null) {
                String name = zipEntry.getName();
                if (name.endsWith(DocumentMetadata.EXTENSION)) {
                    ((MarkLogicDocumentWithMeta) value)
                        .setMeta(getMetadataFromStream());
                    count++;
                    continue;
                }
                //no meta data
                if(count%2 ==0 && !allowEmptyMeta) {
//                    expects meta, while not allowing empty meta
                    LOG.error("Archive damaged: no/incorrect metadata for " + name);
                    return true;
                } else {
                    setKey(zipEntry.getName());
                    readDocFromStream((MarkLogicDocument) value);
                    count++;
                    return true;
                }

            }
        }
        hasNext = false;
        return false;
    }

    private void readDocFromStream(MarkLogicDocument doc) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long size;
        while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, (int) size);
        }
        doc.setContentType(type);
        doc.setContent(baos.toByteArray());
        baos.close();
    }

    private DocumentMetadata getMetadataFromStream() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long size;
        while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, (int) size);
        }
        String metaStr = baos.toString();
        DocumentMetadata metadata = DocumentMetadata.fromXML(new StringReader(
            metaStr));
        baos.close();
        return metadata;
    }
}
