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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicDocument;

/**
 * RecordWriter for <DocumentURI, MarkLogicDocument> creating a single
 * file.
 * 
 * @author jchen
 */
public class SingleDocumentWriter extends 
RecordWriter<DocumentURI, MarkLogicDocument> {
    public static final Log LOG = 
        LogFactory.getLog(SingleDocumentWriter.class);
    
    Path dir;
    Configuration conf;
    
    public SingleDocumentWriter(Path path, Configuration conf) {
        dir = path;
        this.conf = conf;
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
            InterruptedException {  
    }

    @Override
    public void write(DocumentURI uri, MarkLogicDocument content)
            throws IOException, InterruptedException {
        FSDataOutputStream os = null;
        try {
            String uriStr = uri.getUri();
            URI child = new URI(uriStr);
            Path path;
            String childPath;
            if (child.isOpaque()) {
                childPath = child.getSchemeSpecificPart();
            } else {
                childPath = child.getPath();
            }
            if (childPath == null || childPath.isEmpty()) {
                LOG.warn("Error parsing document URI: " + uriStr);
                childPath = uriStr;
            }
            if (childPath.charAt(0) == '/') {
                // concatenate outputPath with path to form the path
                path = new Path(dir.toString() + childPath);
            } else {
                path = new Path(dir, childPath);
            }
            
            FileSystem fs = path.getFileSystem(conf);
            os = fs.create(path, true);
            ContentType type = content.getContentType();
            if (ContentType.BINARY.equals(type)){
                os.write(content.getContentAsByteArray());
            } else if (ContentType.TEXT.equals(type) || 
                    ContentType.XML.equals(type)) {
                Text t = content.getContentAsText();
                os.write(t.toString().getBytes());
            } else {
                LOG.warn("Skipping " + uri + 
                        ".  Unsupported content type: " + 
                        type.name());
            }
        } catch (URISyntaxException e) {
            LOG.warn("Error parsing URI, skipping: " + uri, e);
        } finally {
            if (os != null) {
                os.close();
            }          
        }      
    }
}
