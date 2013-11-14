package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.io.Text;

import com.marklogic.tree.ExpandedTree;

public class RegularBinaryDocument extends BinaryDocument {

    private int[] binaryData;
    private int size;
    
    public RegularBinaryDocument(ExpandedTree tree) {
        size = tree.binaryData.length;
        binaryData = tree.binaryData;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO
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
}
