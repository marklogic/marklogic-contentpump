package com.marklogic.contentpump;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.marklogic.mapreduce.CustomContent;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;

public class ContentWithFileNameWritable<VALUE> implements CustomContent {
    private VALUE value;
    private Text fileName;

    public ContentWithFileNameWritable(VALUE value, String fileName) {
        super();
        this.value = value;
        this.fileName = new Text(fileName);
    }

    public VALUE getValue() {
        return value;
    }

    public void setValue(VALUE value) {
        this.value = value;
    }

    public String getFileName() {
        return fileName.toString();
    }

    public void setFileName(String fileName) {
        this.fileName.set(fileName);
    }

    @Override
    public Content getContent(Configuration conf,
        ContentCreateOptions options, String uri) {
        String[] collections = conf
            .getStrings(MarkLogicConstants.OUTPUT_COLLECTION);
        if (collections != null) {
            List<String> optionList = new ArrayList<String>();
            Collections.addAll(optionList, collections);
            optionList.add(fileName.toString());
            collections = optionList.toArray(new String[0]);
            for (int i = 0; i < collections.length; i++) {
                collections[i] = collections[i].trim();
            }
            options.setCollections(collections);
        } else {
            String[] col = new String[1];
            col[0] = fileName.toString();
            options.setCollections(col);
        }

        Content content = null;
        if (value instanceof Text) {
            content = ContentFactory.newContent(uri,
                ((Text) value).toString(), options);
        } else if (value instanceof MarkLogicNode) {
            content = ContentFactory.newContent(uri,
                ((MarkLogicNode) value).get(), options);
        } else if (value instanceof BytesWritable) {
            content = ContentFactory.newContent(uri,
                ((BytesWritable) value).getBytes(), 0,
                ((BytesWritable) value).getLength(), options);
        }
        return content;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        fileName.readFields(arg0);
        if (value instanceof Text) {
            ((Text) value).readFields(arg0);
        } else if (value instanceof MarkLogicNode) {
            ((MarkLogicNode) value).readFields(arg0);
        } else if (value instanceof BytesWritable) {
            ((BytesWritable) value).readFields(arg0);
        }

    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        fileName.write(arg0);
        if (value instanceof Text) {
            ((Text) value).write(arg0);
        } else if (value instanceof MarkLogicNode) {
            ((MarkLogicNode) value).write(arg0);
        } else if (value instanceof BytesWritable) {
            ((BytesWritable) value).write(arg0);
        }

    }
}
