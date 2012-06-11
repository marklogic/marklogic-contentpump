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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicDocument;

/**
 * Write a document from MarkLogic Server to file system as a separate file. 
 * @author ali
 *
 * 
 */
public class SingleDocumentOutputFormat extends FileOutputFormat<DocumentURI, MarkLogicDocument> {

    @Override
    public RecordWriter<DocumentURI, MarkLogicDocument> getRecordWriter(
        TaskAttemptContext contex) throws IOException, InterruptedException {
        Configuration conf = contex.getConfiguration();
        String p = conf.get(ConfigConstants.CONF_OUTPUT_FILEPATH);
        Path path = new Path(p);
        return new SingleDocumentWriter(path, conf);
    }

}

class SingleDocumentWriter extends RecordWriter<DocumentURI, MarkLogicDocument> {

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
        String pathStr = uri.getUri();
        // hdfs doesn't allow ":" in the file name
        pathStr = dir.toUri().getPath() + "/" + pathStr.replaceAll(":", "");
        
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        FSDataOutputStream out = fs.create(path, false);
        ContentType type = content.getContentType();
        if(ContentType.BINARY.equals(type)){
            out.write(content.getContentAsByteArray());
        } else if(ContentType.TEXT.equals(type) || ContentType.XML.equals(type)) {
            Text t = content.getContentAsText();
            out.write(t.toString().getBytes());
        } else {
            throw new IOException ("incorrect type: " + type);
        }
        out.close();
    }       
}
