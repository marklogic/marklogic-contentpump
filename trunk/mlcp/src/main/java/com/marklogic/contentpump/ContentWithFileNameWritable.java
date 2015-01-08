/*
 * Copyright 2003-2015 MarkLogic Corporation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.marklogic.mapreduce.CustomContent;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
/**
 * Custom content that has stored its own file name.
 * @author ali
 *
 * @param <VALUE>
 */
public class ContentWithFileNameWritable<VALUE> implements CustomContent {
    public static final Log LOG = 
        LogFactory.getLog(ContentWithFileNameWritable.class);
    
    private VALUE value;
    private String fileName;
    /**
     *  type: 0 is text; 1 is MarkLogicNode; 2 is BinaryWritable 
     */
    private byte type;
    
    public ContentWithFileNameWritable() {}
    
    public ContentWithFileNameWritable(VALUE value, String fileName) {
        super();
        this.value = value;
        this.fileName = fileName;
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
        this.fileName = fileName;
    }

    @Override
    public Content getContent(Configuration conf,
        ContentCreateOptions options, String uri) {
        
        String[] collections = conf
            .getStrings(MarkLogicConstants.OUTPUT_COLLECTION);
        
        String collectionUri = null;
        try {
            URI fileUri = new URI(null, null, null, 0, fileName, null, null);
            collectionUri = fileUri.toString();
        } catch (URISyntaxException e) {
            LOG.warn("Error parsing file name as URI " + fileName, e);
        }
        if (collections != null) {
            List<String> optionList = new ArrayList<String>();
            Collections.addAll(optionList, collections);
            if (collectionUri != null) {
                optionList.add(collectionUri);
            }           
            collections = optionList.toArray(new String[0]);
            for (int i = 0; i < collections.length; i++) {
                collections[i] = collections[i].trim();
            }
            options.setCollections(collections);
        } else {
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

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        String fn = Text.readString(in);
        fileName = fn;
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

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, fileName);
        out.writeByte(type);
        if (value instanceof Text) {
            ((Text) value).write(out);
        } else if (value instanceof MarkLogicNode) {
            ((MarkLogicNode) value).write(out);
        } else if (value instanceof BytesWritable) {
            ((BytesWritable) value).write(out);
        }
    }
    
    protected String getEncodedURI(String val) {
        try {
            URI uri = new URI(null, null, null, 0, val, null, null);
            return uri.toString();
        } catch (URISyntaxException e) {
            LOG.warn("Error parsing value as URI, skipping " + val, e);
            return null;
        }
    }
}
