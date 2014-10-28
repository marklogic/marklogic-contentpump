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

import java.io.IOException;
import java.io.InputStream;
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
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;

/**
 * RecordWriter that writes <DocumentURI, MarkLogicDocument> to zip files.
 * 
 * @author jchen
 */
public class ArchiveWriter extends RecordWriter<DocumentURI, MarkLogicDocument>
implements MarkLogicConstants, ConfigConstants {
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
     * Archive for JSON
     */
    private OutputArchive jsonArchive;
    /**
     * Archive for Binary
     */
    private OutputArchive binaryArchive;
    /**
     * is exporting docs
     */
    private boolean isExportDoc;
    private String encoding;
    
    public ArchiveWriter(Path path, TaskAttemptContext context) {
        dir = path.toString();
        this.context = context;
        Configuration conf = context.getConfiguration();
        encoding = conf.get(OUTPUT_CONTENT_ENCODING, DEFAULT_ENCODING);
        String type = conf.get(CONF_OUTPUT_TYPE, DEFAULT_OUTPUT_TYPE);
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
        if (jsonArchive != null) {
            jsonArchive.close();
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
        
        String mode = conf.get(CONF_MODE);
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssZ");
        String timestamp = sdf.format(date);
        if (mode.equals(MODE_DISTRIBUTED)) {
            dst = dir + "/" + context.getTaskAttemptID().getTaskID().getId() 
                + "-" + timestamp + "-" + type.toString();
        } else if (mode.equals(MODE_LOCAL)) {
            dst = dir + "/" + timestamp + "-" + type.toString();
        }
        // Decode URI if exporting documents in compressed form.
        String zipEntryName = isExportDoc ? URIUtil.getPathFromURI(uri) : 
                                            uri.getUri();
        if (zipEntryName == null) {
            if (isExportDoc) {
                LOG.warn("Error parsing URI, skipping: " + uri);
            } else {
                LOG.warn("Found document with empty URI.");
            }
            return;
        }
        if (ContentType.BINARY.equals(type)) {
            if(binaryArchive == null) {
                binaryArchive = new OutputArchive(dst, conf);
            }
            if (!isExportDoc) {
                binaryArchive.write(zipEntryName + DocumentMetadata.EXTENSION,
                    ((DatabaseDocumentWithMeta) content).getMeta().toXML()
                        .getBytes(encoding));
            }
            if (content.isStreamable()) {
                InputStream is = null;
                try {
                    is = content.getContentAsByteStream();
                    long size = content.getContentSize();
                    binaryArchive.write(zipEntryName, 
                            content.getContentAsByteStream(), size);
                } finally {
                    if (is != null) {
                        is.close();
                    }                   
                }
            } else {
                binaryArchive.write(zipEntryName, 
                        content.getContentAsByteArray());
            }
        } else if (ContentType.TEXT.equals(type)) {
            if(txtArchive == null) {
                txtArchive = new OutputArchive(dst, conf);
            }
            if (!isExportDoc) {
                txtArchive.write(zipEntryName + DocumentMetadata.EXTENSION,
                    ((DatabaseDocumentWithMeta) content).getMeta().toXML()
                        .getBytes(encoding));
            }
            String text = content.getContentAsString();
            txtArchive.write(zipEntryName, text.getBytes(encoding));
        } else if (ContentType.XML.equals(type)) {
            if(xmlArchive == null) {
                xmlArchive = new OutputArchive(dst, conf);
            }
            if (!isExportDoc) {
                if (((DatabaseDocumentWithMeta) content).getMeta().isNakedProps) {
                    xmlArchive.write(zipEntryName + DocumentMetadata.NAKED,
                        ((DatabaseDocumentWithMeta) content).getMeta()
                            .toXML().getBytes(encoding));
                } else {
                    xmlArchive.write(
                        zipEntryName + DocumentMetadata.EXTENSION,
                        ((DatabaseDocumentWithMeta) content).getMeta()
                            .toXML().getBytes(encoding));
                    xmlArchive.write(zipEntryName, 
                            content.getContentAsString().getBytes(encoding));
                }
            } else {
                String doc = content.getContentAsString();
                if (doc == null) {
                    LOG.warn("empty document for " + zipEntryName);
                    return;
                }
                xmlArchive.write(zipEntryName, doc.getBytes(encoding));
            }
        } else if (ContentType.JSON.equals(type)) {
            if (jsonArchive == null) {
                jsonArchive = new OutputArchive(dst, conf);
            }
            if (!isExportDoc) {
                jsonArchive.write(zipEntryName + DocumentMetadata.EXTENSION,
                        ((DatabaseDocumentWithMeta) content).getMeta()
                            .toXML().getBytes(encoding));
                jsonArchive.write(zipEntryName, 
                            content.getContentAsString().getBytes(encoding));
            } else {
                String doc = content.getContentAsString();
                if (doc == null) {
                    LOG.warn("empty document for " + zipEntryName);
                    return;
                }
                jsonArchive.write(zipEntryName, doc.getBytes(encoding));
            }
        } else {
            LOG.warn("Skipping " + uri + ".  Unsupported content type: "
                    + type.name());
        }
    }
}
