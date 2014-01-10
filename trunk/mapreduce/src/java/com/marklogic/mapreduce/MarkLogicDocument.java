/*
 * Copyright 2003-2014 MarkLogic Corporation
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

    public ContentType getContentType();

    public Text getContentAsText();

    public byte[] getContentAsByteArray();

    public InputStream getContentAsByteStream();
    
    public MarkLogicNode getContentAsMarkLogicNode();
    
    public String getContentAsString() 
    throws UnsupportedEncodingException;
    
    public long getContentSize();
    
    public boolean isStreamable();

    public void readFields(DataInput in) throws IOException;

    public void write(DataOutput out) throws IOException;
}