/*
 * Copyright (c) 2021 MarkLogic Corporation
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.InternalConstants;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.utilities.URIUtil;

/**
 * RecordWriter for <DocumentURI, MarkLogicDocument> creating a single
 * file.
 * 
 * @author jchen
 */
public class SingleDocumentWriter 
extends RecordWriter<DocumentURI, MarkLogicDocument> 
implements MarkLogicConstants, ConfigConstants, InternalConstants {
    public static final Log LOG = 
        LogFactory.getLog(SingleDocumentWriter.class);
    
    Path dir;
    Configuration conf;
    String encoding;
    public SingleDocumentWriter(Path path, Configuration conf) {
        dir = path;
        this.conf = conf;
        encoding = conf.get(OUTPUT_CONTENT_ENCODING, DEFAULT_ENCODING);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Default charset: " + Charset.defaultCharset());
        }
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
            InterruptedException {  
    }

    @Override
    public void write(DocumentURI uri, MarkLogicDocument content)
            throws IOException, InterruptedException {
        OutputStream os = null;
        try {
            String childPath = URIUtil.getPathFromURI(uri);
            Path path;
            if (childPath.charAt(0) == '/') {
                // concatenate outputPath with path to form the path
                path = new Path(dir.toString() + childPath);
            } else {
                path = new Path(dir, childPath);
            }
            FileSystem fs = path.getFileSystem(conf);
            if (fs instanceof LocalFileSystem) {
                File f = new File(path.toUri().getPath());
                if (!f.exists()) {
                    f.getParentFile().mkdirs();
                    f.createNewFile();
                }
                os = new BufferedOutputStream(new FileOutputStream(f, false));
            } else {
                os = new BufferedOutputStream(fs.create(path, false));
            }

            ContentType type = content.getContentType();
            if (ContentType.BINARY.equals(type)) {
                if (content.isStreamable()) {
                    InputStream is = null;
                    try {
                        is = content.getContentAsByteStream();
                        long size = content.getContentSize();
                        long bufSize = Math.min(size, MAX_BUFFER_SIZE);
                        byte[] buf = new byte[(int)bufSize];
                        for (long toRead = size, read = 0; 
                             toRead > 0; 
                             toRead -= read) {
                            read = is.read(buf, 0, (int)bufSize);
                            if (read > 0) {
                                os.write(buf, 0, (int)read);
                            } else {
                                if (size != Integer.MAX_VALUE) {
                                    LOG.error("Premature EOF: uri=" + uri +
                                        ",toRead=" + toRead);
                                }
                                break;
                            }
                        }
                    } finally {
                       if (is != null) {
                           is.close();
                       }
                    }
                } else {
                    os.write(content.getContentAsByteArray());
                }              
            } else if (ContentType.TEXT.equals(type)
                || ContentType.XML.equals(type)
                || ContentType.JSON.equals(type)) {
                if(encoding.equals("UTF-8")) {
                    Text t = content.getContentAsText();
                    os.write(t.getBytes(), 0, t.getLength());
                } else {
                    String t = content.getContentAsString();
                    os.write(t.getBytes(encoding));
                }
                if (LOG.isTraceEnabled()) {
                	Text t = content.getContentAsText();
                    LOG.trace(t);
                    byte[] bytes = content.getContentAsByteArray();
                    StringBuilder sb = new StringBuilder();
                    for (byte aByte : bytes) {
                        sb.append(Byte.toString(aByte));
                        sb.append(" ");
                    }
                    LOG.trace(sb);
                }
            } else {
                LOG.error("Skipping " + uri + ".  Unsupported content type: "
                    + type.name());
            }
        } catch (Exception e) {
            LOG.error("Error saving: " + uri, e);
        } finally {
            if (os != null) {
                os.close();
            }
        }      
    }
    
    protected static String getPathFromURI(DocumentURI uri)  {
        String uriStr = uri.getUri();
        try {
            URI child = new URI(uriStr);
            String childPath;
            if (child.isOpaque()) {
                childPath = child.getSchemeSpecificPart();
            } else {
                childPath = child.getPath();
            }
            return childPath;
        } catch (Exception ex) {
            LOG.error("Error parsing URI " + uriStr + ".");
            return uriStr;
        }
    }
}
