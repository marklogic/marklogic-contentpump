/*
 * Copyright 2003-2016 MarkLogic Corporation
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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * An abstraction of a document stored in a MarkLogic database.
 * 
 * <p>
 * This interface represents any type of MarkLogic document,
 * regardless of content type or whether it is actively managed
 * by MarkLogic server.
 * </p>
 *  
 * @author jchen
 *
 */
public interface MarkLogicDocument extends Writable {

    /**
     * Return content type of the document.
     * 
     * @return content type of the document.
     */
    public ContentType getContentType();

    /**
     * Return content as Text.  
     * 
     * @return content.
     * @throws UnsupportedOperationException for binary documents.
     */
    public Text getContentAsText();

    /**
     * Return content as byte array.
     * 
     * @return content.
     */
    public byte[] getContentAsByteArray();
    
    /**
     * Return content as byte stream.
     * 
     * @return content.
     */
    public InputStream getContentAsByteStream();
    
    /**
     * Return content as MarkLogicNode.
     * 
     * @return content.
     * @throws UnsupportedOperationException for binary documents.
     */
    public MarkLogicNode getContentAsMarkLogicNode();
    
    /**
     * Return content as String.
     * 
     * @return content.
     * @throws UnsupportedEncodingException
     * @throws UnsupportedOperationException for binary documents.
     */
    public String getContentAsString() 
    throws UnsupportedEncodingException;
    
    /**
     * Return byte length of the content.
     * 
     * @return byte length of the content.
     */
    public long getContentSize();
    
    /**
     * Whether the content can be streamed.
     * 
     * @return true for LargeBinaryDocument, false for the rest.
     */
    public boolean isStreamable();

    @Override
    public void readFields(DataInput in) throws IOException;

    @Override
    public void write(DataOutput out) throws IOException;
}