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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.contentpump.utilities.URIUtil;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicDocument;

/**
 * RecordWriter that writes <DocumentURI, MarkLogicDocument> to zip files.
 * 
 * @author jchen
 */
public class ArchiveWriter extends 
RecordWriter<DocumentURI, MarkLogicDocument> {
    public static final Log LOG = LogFactory.getLog(ArchiveWriter.class);
    private String dir;
    private TaskAttemptContext context;
    /**
     * Archive for Text
     */
    private OutputArchive txtArchive;
    /**
     * Archive for XML
     */
    private OutputArchive xmlArchive;
    /**
     * Archive for Binary
     */
    private OutputArchive binaryArchive;
    /**
     * is exporting docs
     */
    private boolean isExportDoc;

    public ArchiveWriter(Path path, TaskAttemptContext context) {
        dir = path.toString();
        this.context = context;
        Configuration conf = context.getConfiguration();
        String type = conf.get(ConfigConstants.CONF_OUTPUT_TYPE, ConfigConstants.DEFAULT_OUTPUT_TYPE);
        ExportOutputType outputType = ExportOutputType.valueOf(
                        type.toUpperCase());
        if (outputType.equals(ExportOutputType.DOCUMENT)) {
            isExportDoc = true;
        } else {
            //archive uses DatabaseContentReader
            isExportDoc = false;
        }
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
        InterruptedException {
        if (txtArchive != null) {
            txtArchive.close();
        }
        if (xmlArchive != null) {
            xmlArchive.close();
        }
        if (binaryArchive != null) {
            binaryArchive.close();
        }
    }

    @Override
    public void write(DocumentURI uri, MarkLogicDocument content)
        throws IOException, InterruptedException {
        ContentType type = content.getContentType();
        if(type == null) {
            throw new IOException ("null content type: ");
        }
        Configuration conf = context.getConfiguration();
        String dst = null;
        
        String mode = conf.get(ConfigConstants.CONF_MODE);
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssZ");
        String timestamp = sdf.format(date);
        if (mode.equals(ConfigConstants.MODE_DISTRIBUTED)) {
            dst = dir + "/" + context.getTaskAttemptID().getTaskID().getId() 
                + "-" + timestamp + "-" + type.toString();
        } else if (mode.equals(ConfigConstants.MODE_LOCAL)) {
            dst = dir + "/" + timestamp + "-" + type.toString();
        }
        String zipEntryName = URIUtil.getPathFromURI(uri);
        if (zipEntryName == null) {
            LOG.warn("Error parsing URI, skipping: " + uri);
            return;
        }
        if (ContentType.BINARY.equals(type)) {
            if(binaryArchive == null) {
                binaryArchive = new OutputArchive(dst, conf);
            }
            if (!isExportDoc) {
                binaryArchive.write(zipEntryName + DocumentMetadata.EXTENSION,
                    ((MarkLogicDocumentWithMeta) content).getMeta().toXML()
                        .getBytes());
            }
            binaryArchive.write(zipEntryName, 
                    content.getContentAsByteArray());
        } else if(ContentType.TEXT.equals(type)) {
            if(txtArchive == null) {
                txtArchive = new OutputArchive(dst, conf);
            }
            if (!isExportDoc) {
                txtArchive.write(zipEntryName + DocumentMetadata.EXTENSION,
                    ((MarkLogicDocumentWithMeta) content).getMeta().toXML()
                        .getBytes());
            }
            txtArchive.write(zipEntryName, 
                    content.getContentAsText().getBytes());
        } else if(ContentType.XML.equals(type)) {
            if(xmlArchive == null) {
                xmlArchive = new OutputArchive(dst, conf);
            }
            if (!isExportDoc) {
                if (((MarkLogicDocumentWithMeta) content).getMeta().isNakedProps) {
                    xmlArchive.write(zipEntryName + DocumentMetadata.NAKED,
                        ((MarkLogicDocumentWithMeta) content).getMeta()
                            .toXML().getBytes());
                } else {
                    xmlArchive.write(
                        zipEntryName + DocumentMetadata.EXTENSION,
                        ((MarkLogicDocumentWithMeta) content).getMeta()
                            .toXML().getBytes());
                    xmlArchive.write(zipEntryName, content.getContentAsText()
                        .getBytes());
                }
            } else {
                xmlArchive.write(zipEntryName, content.getContentAsText()
                    .getBytes());
            }
        } else {
            throw new IOException ("incorrect type: " + type);
        }
    }
}
