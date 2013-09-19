/*
 * Copyright 2003-2011 MarkLogic Corporation
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
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XdmBinary;

/**
 * Captures any type of MarkLogic documents.
 *  
 * @author jchen
 *
 */
public class MarkLogicDocument implements Writable {
    private byte[] content;
    private ContentType contentType;
    public static final Log LOG = LogFactory.getLog(MarkLogicDocument.class);
    public ContentType getContentType() {
        return contentType;
    }
    
    public Text getContentAsText() {
        if (contentType == ContentType.XML || 
            contentType == ContentType.TEXT) {
            return new Text(content);         
        }
        throw new UnsupportedOperationException(
        "Cannot convert binary data to Text.");
    }
    
    public String getContentAsString() throws UnsupportedEncodingException {
        if (contentType == ContentType.XML || 
            contentType == ContentType.TEXT) {
            return new String(content, "UTF-8");         
        }
        throw new UnsupportedOperationException(
        "Cannot convert binary data to Text.");
    }
    
    public byte[] getContentAsByteArray() {
        return content;
    }
    
    public MarkLogicNode getContentAsMarkLogicNode() {
        if (contentType == ContentType.XML || 
            contentType == ContentType.TEXT) {
            return new MarkLogicNode(getContentAsText().toString(), 
                            contentType);      
        }
        throw new UnsupportedOperationException(
                            "Cannot convert binary data to Text.");        
    }
    
    public void set(ResultItem item){
        try {
            if (item.getValueType() == ValueType.DOCUMENT) {
                content = item.asString().getBytes("UTF-8");
                contentType = ContentType.XML;
            } else if (item.getValueType() == ValueType.ELEMENT) {
                    content = item.asString().getBytes("UTF-8");
                    contentType = ContentType.XML;
            } else if (item.getValueType() == ValueType.TEXT) {
                content = item.asString().getBytes("UTF-8");
                contentType = ContentType.TEXT;
            } else if (item.getValueType() == ValueType.BINARY) {
                content = ((XdmBinary) item.getItem()).asBinaryData();
                contentType = ContentType.BINARY;
            } else {
                contentType = ContentType.UNKNOWN;
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error(e);
        }
    }
    
    public void setContent(byte[] content) {
        this.content = content;
    }
    
    public void setContentType(ContentType type) {
        contentType = type;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int ordinal = in.readInt();
        contentType = ContentType.valueOf(ordinal);
        int length = WritableUtils.readVInt(in);
        content = new byte[length];
        in.readFully(content, 0, length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(contentType.ordinal());
        WritableUtils.writeVInt(out, content.length);
        out.write(content, 0, content.length);
    }
}
