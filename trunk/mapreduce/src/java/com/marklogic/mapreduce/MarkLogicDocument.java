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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Captures any type of MarkLogic documents.
 *  
 * @author jchen
 *
 */
public interface MarkLogicDocument extends Writable {

    public abstract ContentType getContentType();

    public abstract Text getContentAsText();

    public abstract byte[] getContentAsByteArray();

    public abstract MarkLogicNode getContentAsMarkLogicNode();
    
    public abstract String getContentAsString() 
    throws UnsupportedEncodingException;

    public abstract void readFields(DataInput in) throws IOException;

    public abstract void write(DataOutput out) throws IOException;
}