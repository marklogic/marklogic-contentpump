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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.io.Text;

import com.marklogic.tree.ExpandedTree;

/**
 * A {@link BinaryDocument} representing a binary fragment in MarkLogic
 * accessed via Direct Access.
 * 
 * @see ForestInputFormat
 * @author jchen
 */
public class RegularBinaryDocument extends BinaryDocument {

    private int[] binaryData;
    private int size = 0;
    
    public RegularBinaryDocument() {
    }
    
    public RegularBinaryDocument(ExpandedTree tree) {
        size = tree.binaryData.length;
        binaryData = tree.binaryData;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        size = in.readInt();
        binaryData = new int[size];
        for (int i = 0; i < size; i++) {
            binaryData[i] = in.readInt();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeInt(binaryData[i]);
        }
    }

    @Override
    public byte[] getContentAsByteArray() {
        ByteBuffer buf = ByteBuffer.allocate(size * 4);
        buf.order(ByteOrder.LITTLE_ENDIAN); 
        for (int i : binaryData) {
            buf.putInt(i);
        }
        return buf.array();
    }

    @Override
    public MarkLogicNode getContentAsMarkLogicNode() {
        throw new UnsupportedOperationException(
        "Cannot convert binary data to MarkLogicNode.");
    }

    @Override
    public Text getContentAsText() {
        throw new UnsupportedOperationException(
        "Cannot convert binary data to Text.");
    }

    @Override
    public ContentType getContentType() {
        return ContentType.BINARY;
    }

    @Override
    public String getContentAsString() throws UnsupportedEncodingException {
        throw new UnsupportedOperationException(
                "Cannot convert binary data to String.");
    }
    
    @Override
    public long getContentSize() {
        return size * 4;
    }
}
