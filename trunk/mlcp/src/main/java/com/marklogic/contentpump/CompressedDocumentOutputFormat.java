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

public class CompressedDocumentOutputFormat extends FileOutputFormat<DocumentURI, MarkLogicDocument> {
    @Override
    public RecordWriter<DocumentURI, MarkLogicDocument> getRecordWriter(
        TaskAttemptContext contex) throws IOException, InterruptedException {
        Configuration conf = contex.getConfiguration();
        Path path = new Path(conf.get(ConfigConstants.CONF_OUTPUT_FILEPATH));
        return new DocumentZipWriter(path, conf);
    }
}

class DocumentZipWriter extends RecordWriter<DocumentURI, MarkLogicDocument> {
    private String dir;
    private Configuration conf;
    private OutputArchive zip;
    
    public DocumentZipWriter(Path dir, Configuration conf) {
        this.dir = dir.toUri().getPath();
        this.conf = conf;
    }

    @Override
    public void write(DocumentURI uri, MarkLogicDocument content)
        throws IOException, InterruptedException {
        ContentType type = content.getContentType();
        if(type == null) {
            throw new IOException ("null content type: ");
        }
        if(zip == null) {
            zip = new OutputArchive(dir, conf);
        }
        if(ContentType.BINARY.equals(type)) {
            zip.write(uri.getUri(), content.getContentAsByteArray());
        } else if(ContentType.TEXT.equals(type)) {
            zip.write(uri.getUri(), content.getContentAsText().toString().getBytes());
        } else if(ContentType.XML.equals(type)) {
            zip.write(uri.getUri(), content.getContentAsText().toString().getBytes());
        } else {
            throw new IOException ("incorrect type: " + type);
        }
        
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
        if(zip != null) {
            zip.close();
        }
        
    }
    
}