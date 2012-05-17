package com.marklogic.contentpump;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.marklogic.mapreduce.MarkLogicNode;

public enum SequenceFileValueType {
    MARKLOGICNODE {
        @Override
        public Class<? extends Writable> getWritableClass() {
            return MarkLogicNode.class;
        }
    },
    TEXT {
        @Override
        public Class<? extends Writable> getWritableClass() {
            return Text.class;
        }
    },
    BYTESWRITABLE {
        @Override
        public Class<? extends Writable> getWritableClass() {
            return BytesWritable.class;
        }
    };
    public abstract Class<? extends Writable> getWritableClass();
}
