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
    /**
     *  type: 0 is text; 1 is MarkLogicNode; 2 is BinaryWritable 
     */
    private byte type;
    public ContentWithFileNameWritable(VALUE value, String fileName) {
        super();
        this.value = value;
        this.fileName = new Text(fileName);
        if (value instanceof Text) {
            type = 0;
        } else if (value instanceof MarkLogicNode) {
            type = 1;
        } else if (value instanceof BytesWritable) {
            type = 2;
        }
    }

    public VALUE getValue() {
        return value;
    }

    public void setValue(VALUE value) {
        this.value = value;
        if (value instanceof Text) {
            type = 0;
        } else if (value instanceof MarkLogicNode) {
            type = 1;
        } else if (value instanceof BytesWritable) {
            type = 2;
        }
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
    public void readFields(DataInput in) throws IOException {
        String fn = Text.readString(in);
        fileName.set(fn);
        byte valueType = in.readByte();
        switch (valueType) {
        case 0:
            ((Text) value).readFields(in);
            break;
        case 1:
            ((MarkLogicNode) value).readFields(in);
            break;
        case 2:
            ((BytesWritable) value).readFields(in);
            break;
        default:
            throw new IOException("incorrect type");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fileName.write(out);
        out.writeByte(type);
        if (value instanceof Text) {
            ((Text) value).write(out);
        } else if (value instanceof MarkLogicNode) {
            ((MarkLogicNode) value).write(out);
        } else if (value instanceof BytesWritable) {
            ((BytesWritable) value).write(out);
        }
    }
    
    public static void main(String [] args) {
        
    }
}
