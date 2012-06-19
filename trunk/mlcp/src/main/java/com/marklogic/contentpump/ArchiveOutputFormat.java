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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicDocument;

public class ArchiveOutputFormat extends
    FileOutputFormat<DocumentURI, MarkLogicDocument> {
    @Override
    public RecordWriter<DocumentURI, MarkLogicDocument> getRecordWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path path = new Path(conf.get(ConfigConstants.CONF_OUTPUT_FILEPATH));
        return new ArchiveWriter(path, context);
    }

}

class ArchiveWriter extends RecordWriter<DocumentURI, MarkLogicDocument> {

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

    public ArchiveWriter(Path path, TaskAttemptContext context) {
        dir = path.toUri().getPath();
        this.context = context;
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
        if (mode.equals(ConfigConstants.MODE_DISTRIBUTED)) {
            dst = dir + "-" + context.getTaskAttemptID().getTaskID().getId()
                + "." + type.toString();
        } else if (mode.equals(ConfigConstants.MODE_LOCAL)) {
            dst = dir + "." + type.toString();
        }
        
        if(ContentType.BINARY.equals(type)) {
            if(binaryArchive == null) {
                binaryArchive = new OutputArchive(dst, conf);
            }
            binaryArchive.write(uri.getUri(), content.getContentAsByteArray());
        } else if(ContentType.TEXT.equals(type)) {
            if(txtArchive == null) {
                txtArchive = new OutputArchive(dst, conf);
            }
            txtArchive.write(uri.getUri(), content.getContentAsText().toString().getBytes());
        } else if(ContentType.XML.equals(type)) {
            if(xmlArchive == null) {
                xmlArchive = new OutputArchive(dst, conf);
            }
            xmlArchive.write(uri.getUri(), content.getContentAsText().toString().getBytes());
        } else {
            throw new IOException ("incorrect type: " + type);
        }
    }
}
