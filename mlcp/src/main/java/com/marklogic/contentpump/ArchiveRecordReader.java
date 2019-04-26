/*
 * Copyright 2003-2019 MarkLogic Corporation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DatabaseDocument;

/**
 * Read archive, construct MarkLogicDocumentWithMeta as value.
 * 
 * @author ali
 *
 */
public class ArchiveRecordReader extends
    ImportRecordReader<DatabaseDocumentWithMeta> implements 
    ConfigConstants {
    public static final Log LOG = LogFactory.getLog(ArchiveRecordReader.class);
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
        initConfig(context);
        allowEmptyMeta = conf.getBoolean(
            CONF_INPUT_ARCHIVE_METADATA_OPTIONAL, false);
         
        setFile(((FileSplit) inSplit).getPath());
        fs = file.getFileSystem(context.getConfiguration());
        FileStatus status = fs.getFileStatus(file);
        if(status.isDirectory()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        initStream(inSplit);
    }
    
    protected void initStream(InputSplit inSplit) throws IOException {
        FSDataInputStream fileIn = openFile(inSplit, false);
        if (fileIn == null) {
            return;
        }
        int index = file.toUri().getPath().lastIndexOf(EXTENSION);
        String subStr = file.toUri().getPath().substring(0, index);
        index = subStr.lastIndexOf('-');
        String typeStr = subStr.substring(index + 1, subStr.length());
        type = ContentType.valueOf(typeStr);
        value = new DatabaseDocumentWithMeta();
        zipIn = new ZipInputStream(fileIn);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }

        ZipEntry zipEntry;
        ZipInputStream zis = (ZipInputStream) zipIn;
        if (value == null) {
            value = new DatabaseDocumentWithMeta();
        }
        while ((zipEntry = zis.getNextEntry()) != null) {
            subId = zipEntry.getName();
            long length = zipEntry.getSize();
            if (subId.endsWith(DocumentMetadata.NAKED)) {
                ((DatabaseDocumentWithMeta) value)
                    .setMeta(getMetadataFromStream(length));
                String uri = subId.substring(0, subId.length()
                        - DocumentMetadata.NAKED.length());
                setKey(uri, 0, 0, false);
                value.setContent(null);
                count++;
                return true;
            }
            if (count % 2 == 0 && subId.endsWith(DocumentMetadata.EXTENSION)) {
                ((DatabaseDocumentWithMeta) value)
                    .setMeta(getMetadataFromStream(length));
                count++;
                continue;
            }
            // no meta data
            if (count % 2 == 0 && !allowEmptyMeta) {
                setSkipKey(0, 0, "Missing metadata");
                return true;
            } else {
                setKey(subId, 0, 0, false);
                readDocFromStream(length, (DatabaseDocument) value);
                count++;
                return true;
            }
        }
        //end of a zip
        if (iterator != null && iterator.hasNext()) {
            close();
            initStream(iterator.next());
            return nextKeyValue();
        } else {
            hasNext = false;
            return false;
        }
    }

    private void readDocFromStream(long entryLength, DatabaseDocument doc)
        throws IOException {
        ByteArrayOutputStream baos;
        if (entryLength == -1) {
            baos = new ByteArrayOutputStream();
        } else {
            baos = new ByteArrayOutputStream((int) entryLength);
        }
        int size;
        while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, size);
        }
        doc.setContentType(type);
        doc.setContent(baos.toByteArray());
        baos.close();
    }

    private DocumentMetadata getMetadataFromStream(long entryLength)
        throws IOException {
        ByteArrayOutputStream baos;
        if (entryLength == -1) {
            baos = new ByteArrayOutputStream();
        } else {
            baos = new ByteArrayOutputStream((int) entryLength);
        }
        int size;
        while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, size);
        }

        DocumentMetadata metadata = DocumentMetadata.fromXML(new StringReader(
            baos.toString()));
        baos.close();
        return metadata;
    }
}
