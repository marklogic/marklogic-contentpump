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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.MarkLogicNode;

/**
 * Write a document from MarkLogic Server to file system as a separate file. 
 * @author ali
 *
 * @param <BytesWritable>
 */
public class SingleDocumentOutputFormat extends FileOutputFormat<DocumentURI, MarkLogicDocument> {

    @Override
    public RecordWriter<DocumentURI, MarkLogicDocument> getRecordWriter(
        TaskAttemptContext contex) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        Configuration conf = contex.getConfiguration();
        Path path = new Path(conf.get(MarkLogicConstants.OUTPUT_DIRECTORY));
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
        int index = uri.getUri().lastIndexOf("/");
        String pathStr = null;
        if(index == -1) {
            pathStr = dir.toUri().getPath() + "/" + uri.getUri();
        } else {
            pathStr = dir.toUri().getPath() + uri.getUri().substring(index);
        }
        
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        FSDataOutputStream out = fs.create(path, false);
        System.out.println("writing to: " + path);
//        if(content instanceof BytesWritable) {
//            ((BytesWritable)content).write(out);
//        } else if (content instanceof MarkLogicNode) {
//            String s = ((MarkLogicNode)content).toString();
//            out.write(s.getBytes());
//        } else if (content instanceof Text) {
//            ((Text)content).write(out);
//        }
//        out.write(content.getBytes(), 0, content.getLength());
//        out.flush();
        ContentType type = content.getContentType();
        if(ContentType.BINARY.equals(type)){
            out.write(content.getContentAsByteArray());
        } else if(ContentType.TEXT.equals(type) || ContentType.XML.equals(type)) {
            Text t = content.getContentAsText(); ;
            out.write(t.toString().getBytes());
        } else {
            throw new IOException ("incorrect type: " + type);
        }
        out.close();
    }       
}
