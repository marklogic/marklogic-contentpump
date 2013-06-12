package com.marklogic.contentpump;

import com.marklogic.mapreduce.CustomContent;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ndw
 * Date: 6/12/13
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class RDFWritable<VALUE> implements CustomContent {
    private VALUE value;
    private String collectionUri = null;
    private byte type = 0; // Triples are always text

    public RDFWritable() {
    }

    public void set(String value) {
        this.value = (VALUE) new Text((String) value);
    }

    public void setCollection(String collection) {
        collectionUri = collection;
    }

    public VALUE getValue() {
        return value;
    }

    @Override
    public Content getContent(Configuration conf, ContentCreateOptions options, String uri) {
        String[] collections = conf.getStrings(MarkLogicConstants.OUTPUT_COLLECTION);

        if (collections != null) {
            List<String> optionList = new ArrayList<String>();
            Collections.addAll(optionList, collections);
            collections = optionList.toArray(new String[0]);
            for (int i = 0; i < collections.length; i++) {
                collections[i] = collections[i].trim();
            }
            options.setCollections(collections);
        } else {
            if (collectionUri == null) {
                collectionUri = "http://marklogic.com/semantics#default-graph";
            }
            String[] col = new String[1];
            col[0] = collectionUri;
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
    public void write(DataOutput out) throws IOException {
        if (collectionUri == null) {
            out.writeByte(0);
        } else {
            out.writeByte(1);
            Text t = new Text(collectionUri);
            t.write(out);
        }
        out.writeByte(type);
        if (value instanceof Text) {
            ((Text) value).write(out);
        } else if (value instanceof MarkLogicNode) {
            ((MarkLogicNode) value).write(out);
        } else if (value instanceof BytesWritable) {
            ((BytesWritable) value).write(out);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        byte hasCollection = in.readByte();
        if (hasCollection != 0) {
            Text t = new Text();
            t.readFields(in);
            collectionUri = t.toString();
        }
        byte valueType = in.readByte();
        switch (valueType) {
            case 0:
                value = (VALUE) new Text();
                ((Text) value).readFields(in);
                break;
            case 1:
                value = (VALUE) new MarkLogicNode();
                ((MarkLogicNode) value).readFields(in);
                break;
            case 2:
                value = (VALUE) new BytesWritable();
                ((BytesWritable) value).readFields(in);
                break;
            default:
                throw new IOException("incorrect type");
        }
        type = valueType;
    }
}
