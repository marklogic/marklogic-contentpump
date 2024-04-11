/*
 * Copyright (c) 2021 MarkLogic Corporation
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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import com.marklogic.io.IOHelper;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XdmBinary;

/**
 * A {@link MarkLogicDocument} retrieved from a database via
 * MarkLogic Server.
 *  
 * @author jchen
 *
 */
public class DatabaseDocument implements MarkLogicDocument,
InternalConstants {
    public static final Log LOG = LogFactory.getLog(
            DatabaseDocument.class);
    protected byte[] content;
    protected InputStream is; // streaming binary
    protected ContentType contentType;
    
    public DatabaseDocument(){}
    
    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#getContentType()
     */
    public ContentType getContentType() {
        return contentType;
    }
    
    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#getContentAsText()
     */
    public Text getContentAsText() {
        if (contentType == ContentType.XML || 
            contentType == ContentType.JSON ||
            contentType == ContentType.TEXT) {
            return new Text(content);         
        }
        throw new UnsupportedOperationException(
        "Cannot convert binary data to Text.");
    }
    
    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#getContentAsByteArray()
     */
    public byte[] getContentAsByteArray() {
        if (content == null) {
            try {
                content = IOHelper.byteArrayFromStream(is);
            } catch (IOException e) {
                throw new RuntimeException("IOException buffering binary data",
                        e);
            }
            is = null;
        }
        return content;
    }
    
    @Override
    public InputStream getContentAsByteStream() {
        if (is != null) {
            return is;
        }
        return new ByteArrayInputStream(getContentAsByteArray());
    }
    
    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#getContentAsMarkLogicNode()
     */
    public MarkLogicNode getContentAsMarkLogicNode() {
        if (contentType == ContentType.XML || 
            contentType == ContentType.TEXT) {
            return new MarkLogicNode(getContentAsText().toString(), 
                            contentType);      
        }
        throw new UnsupportedOperationException(
            "Cannot convert JSON or binary data to MarkLogicNode.");        
    }
    
    public String getContentAsString() throws UnsupportedEncodingException {
        if (contentType == ContentType.XML || 
            contentType == ContentType.JSON ||
            contentType == ContentType.TEXT) {
            return new String(content, "UTF-8");         
        }
        throw new UnsupportedOperationException(
        "Cannot convert binary data to String.");
    }
    
    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#set(com.marklogic.xcc.ResultItem)
     */
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
                if (item.isCached()) {
                    content = ((XdmBinary) item.getItem()).asBinaryData();
                } else {
                    is = item.asInputStream();
                }
                contentType = ContentType.BINARY;
            } else if (item.getValueType() == ValueType.ARRAY_NODE ||
                item.getValueType() == ValueType.BOOLEAN_NODE ||
                item.getValueType() == ValueType.NULL_NODE ||
                item.getValueType() == ValueType.NUMBER_NODE ||
                item.getValueType() == ValueType.OBJECT_NODE) {
                content = item.asString().getBytes("UTF-8");
                contentType = ContentType.JSON;
            } else {
                contentType = ContentType.UNKNOWN;
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error(e);
        }
    }
    
    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#setContent(byte[])
     */
    public void setContent(byte[] content) {
        this.content = content;
    }
    
    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#setContentType(com.marklogic.mapreduce.ContentType)
     */
    public void setContentType(ContentType type) {
        contentType = type;
    }

    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int ordinal = in.readInt();
        contentType = ContentType.valueOf(ordinal);
        int length = WritableUtils.readVInt(in);
        if (length > MAX_BUFFER_SIZE) {
            is = (DataInputStream)in;
            return;
        }
        content = new byte[length];
        in.readFully(content, 0, length);
    }

    /* (non-Javadoc)
     * @see com.marklogic.mapreduce.MarkLogicDocument#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(contentType.ordinal());
        if (content != null) {
            WritableUtils.writeVInt(out, content.length);
            out.write(content, 0, content.length);
        } else if (is != null) {
            content = new byte[MAX_BUFFER_SIZE];
            int len = 0;
            while ((len = is.read(content)) > 0) {
                out.write(content, 0, len);
            }
        }      
    }

    @Override
    public long getContentSize() {
        if (content != null) {
            return content.length;
        } else {
            return Integer.MAX_VALUE;
        }
    }
    
    @Override
    public boolean isStreamable() {
        return content == null;
    }
}
